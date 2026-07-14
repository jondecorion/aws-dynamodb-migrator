import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("dynamodb_migrator")


class CheckpointManager:
    def __init__(self, filepath: str = "./.ddb_migration_checkpoint.json"):
        self.filepath = filepath
        self.lock = threading.Lock()
        self.state = {}

    def load(self) -> Dict[str, Any]:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning("Could not load checkpoint file: %s", e)
        return {}

    def save(
        self,
        src_table: str,
        tgt_table: str,
        total_segments: int,
        segment_id: int,
        last_key: Optional[Dict[str, Any]]
    ) -> None:
        with self.lock:
            # Read existing state if file exists to prevent overwriting other threads' state
            if os.path.exists(self.filepath):
                try:
                    with open(self.filepath, "r", encoding="utf-8") as f:
                        self.state = json.load(f)
                except Exception:
                    pass

            self.state["src_table"] = src_table
            self.state["tgt_table"] = tgt_table
            self.state["total_segments"] = total_segments
            
            if "segments" not in self.state:
                self.state["segments"] = {}
                
            if last_key:
                self.state["segments"][str(segment_id)] = last_key
            else:
                self.state["segments"][str(segment_id)] = "COMPLETE"

            try:
                with open(self.filepath, "w", encoding="utf-8") as f:
                    json.dump(self.state, f, indent=4)
            except Exception as e:
                logger.warning("Could not write checkpoint file: %s", e)

    def delete(self) -> None:
        with self.lock:
            if os.path.exists(self.filepath):
                try:
                    os.remove(self.filepath)
                    logger.info("Cleared migration checkpoint file.")
                except Exception as e:
                    logger.warning("Could not delete checkpoint file: %s", e)


class ProgressTracker:
    def __init__(self, expected_total: int):
        self.lock = threading.Lock()
        self.count = 0
        self.expected_total = expected_total
        self.last_reported = 0

    def increment(self, amount: int = 1):
        with self.lock:
            self.count += amount
            # Report progress every 1000 items, or at the very end
            if (self.count - self.last_reported >= 1000) or (self.count >= self.expected_total):
                if self.expected_total > 0:
                    pct = (self.count / self.expected_total) * 100
                    logger.info("  Progress: %d / %d items copied (%.1f%%)", self.count, self.expected_total, pct)
                else:
                    logger.info("  Progress: %d items copied", self.count)
                self.last_reported = self.count


def setup_logging(verbose: bool = False) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    
    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(handler)


def get_interactive_choice(options: List[str], prompt: str) -> str:
    print(f"\n{prompt}")
    for idx, opt in enumerate(options, 1):
        print(f"  [{idx}] {opt}")
    while True:
        val = input("Enter choice number: ").strip()
        try:
            choice_idx = int(val) - 1
            if 0 <= choice_idx < len(options):
                return options[choice_idx]
        except ValueError:
            pass
        print(f"Invalid option. Please choose a number from 1 to {len(options)}")


def get_input(prompt: str, default: Optional[str] = None) -> str:
    display_prompt = f"{prompt} [{default}]: " if default else f"{prompt}: "
    val = input(display_prompt).strip()
    return val if val else (default or "")


def choose_aws_profile(prompt: str) -> str:
    try:
        session = boto3.Session()
        profiles = session.available_profiles
    except Exception:
        profiles = []
        
    if not profiles:
        return get_input(prompt, default="default")
        
    options = profiles.copy()
    options.append("Enter profile manually")
    
    selected = get_interactive_choice(options, f"Select {prompt}:")
    if selected == "Enter profile manually":
        return get_input(prompt)
    return selected


def choose_ddb_table(session: boto3.Session, region: str, prompt: str) -> str:
    try:
        client = session.client("dynamodb", region_name=region)
        tables_resp = client.list_tables(Limit=100)
        tables = tables_resp.get("TableNames", [])
    except Exception as e:
        logger.warning("Could not list DynamoDB tables automatically: %s", e)
        tables = []
        
    if not tables:
        return get_input(prompt)
        
    options = tables.copy()
    options.append("Enter Table Name manually")
    
    selected = get_interactive_choice(options, f"Select {prompt}:")
    if selected == "Enter Table Name manually":
        return get_input(prompt)
    return selected


def parse_billing_and_capacity(desc: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    billing_mode = "PROVISIONED"
    if "BillingModeSummary" in desc:
        billing_mode = desc["BillingModeSummary"].get("BillingMode", "PROVISIONED")
    elif "ProvisionedThroughput" in desc:
        throughput = desc["ProvisionedThroughput"]
        if throughput.get("ReadCapacityUnits", 0) == 0 and throughput.get("WriteCapacityUnits", 0) == 0:
            billing_mode = "PAY_PER_REQUEST"
            
    if billing_mode == "PAY_PER_REQUEST":
        return "PAY_PER_REQUEST", None
    else:
        throughput = desc.get("ProvisionedThroughput", {})
        return "PROVISIONED", {
            "ReadCapacityUnits": throughput.get("ReadCapacityUnits", 1),
            "WriteCapacityUnits": throughput.get("WriteCapacityUnits", 1)
        }


def replicate_table_schema(
    tgt_client: Any,
    src_table_desc: Dict[str, Any],
    tgt_table_name: str
) -> Tuple[str, int]:
    logger.info("Parsing source table configuration for recreation...")
    desc = src_table_desc["Table"]
    
    # Reconstruct AttributeDefinitions and KeySchema
    create_params = {
        "TableName": tgt_table_name,
        "AttributeDefinitions": desc["AttributeDefinitions"],
        "KeySchema": desc["KeySchema"]
    }
    
    billing_mode, provisioned_throughput = parse_billing_and_capacity(desc)
    
    if billing_mode == "PAY_PER_REQUEST":
        create_params["BillingMode"] = "PAY_PER_REQUEST"
        logger.info("Target Table Capacity Mode: On-Demand (PAY_PER_REQUEST)")
        wcu = 0
    else:
        create_params["BillingMode"] = "PROVISIONED"
        create_params["ProvisionedThroughput"] = provisioned_throughput
        wcu = provisioned_throughput["WriteCapacityUnits"] if provisioned_throughput else 1
        logger.info("Target Table Capacity Mode: Provisioned (RCU: %d, WCU: %d)", 
                    create_params["ProvisionedThroughput"]["ReadCapacityUnits"], wcu)
        
    # Replicate Global Secondary Indexes (GSIs)
    if "GlobalSecondaryIndexes" in desc:
        gsi_list = []
        for index in desc["GlobalSecondaryIndexes"]:
            cleaned_index = {
                "IndexName": index["IndexName"],
                "KeySchema": index["KeySchema"],
                "Projection": index["Projection"]
            }
            if billing_mode == "PROVISIONED":
                idx_throughput = index.get("ProvisionedThroughput", {})
                cleaned_index["ProvisionedThroughput"] = {
                    "ReadCapacityUnits": idx_throughput.get("ReadCapacityUnits", 1),
                    "WriteCapacityUnits": idx_throughput.get("WriteCapacityUnits", 1)
                }
            gsi_list.append(cleaned_index)
        create_params["GlobalSecondaryIndexes"] = gsi_list
        logger.info("Replicating %d Global Secondary Indexes...", len(gsi_list))
        
    # Replicate Stream Settings
    if "StreamSpecification" in desc and desc["StreamSpecification"].get("StreamEnabled", False):
        create_params["StreamSpecification"] = {
            "StreamEnabled": True,
            "StreamViewType": desc["StreamSpecification"]["StreamViewType"]
        }
        logger.info("Replicating Stream Settings (ViewType: %s)", create_params["StreamSpecification"]["StreamViewType"])
        
    logger.info("Creating DynamoDB table '%s' in target account...", tgt_table_name)
    try:
        tgt_client.create_table(**create_params)
        logger.info("Table creation request submitted successfully.")
        return billing_mode, wcu
    except ClientError as e:
        logger.error("Failed to create DynamoDB table: %s", e.response["Error"]["Message"])
        sys.exit(1)


def migrate_segment(
    src_profile: str,
    src_region: str,
    src_table_name: str,
    tgt_profile: str,
    tgt_region: str,
    tgt_table_name: str,
    segment_id: int,
    total_segments: int,
    progress_tracker: ProgressTracker,
    checkpoint_mgr: CheckpointManager,
    initial_key: Optional[Dict[str, Any]]
) -> int:
    # Spawn dedicated sessions and clients for this thread
    session_src = boto3.Session(profile_name=src_profile, region_name=src_region)
    session_tgt = boto3.Session(profile_name=tgt_profile, region_name=tgt_region)
    
    src_table = session_src.resource("dynamodb").Table(src_table_name)
    tgt_table = session_tgt.resource("dynamodb").Table(tgt_table_name)
    
    copied = 0
    scan_kwargs = {}
    
    # Configure segmented scan parameters if total segments > 1
    if total_segments > 1:
        scan_kwargs["Segment"] = segment_id
        scan_kwargs["TotalSegments"] = total_segments
        
    if initial_key:
        scan_kwargs["ExclusiveStartKey"] = initial_key
        logger.info("[Segment %d/%d] Resuming scan from saved checkpoint.", segment_id + 1, total_segments)
        
    try:
        with tgt_table.batch_writer() as batch:
            while True:
                response = src_table.scan(**scan_kwargs)
                items = response.get("Items", [])
                
                for item in items:
                    batch.put_item(Item=item)
                    copied += 1
                    progress_tracker.increment()
                    
                last_key = response.get("LastEvaluatedKey")
                # Save progress checkpoint to local file
                checkpoint_mgr.save(src_table_name, tgt_table_name, total_segments, segment_id, last_key)
                
                scan_kwargs["ExclusiveStartKey"] = last_key
                if not last_key:
                    break
                    
        return copied
    except ClientError as e:
        logger.error("[Segment %d/%d] Error during migration: %s", 
                     segment_id + 1, total_segments, e.response["Error"]["Message"])
        raise e


def determine_concurrency(item_count: int, size_bytes: int, billing_mode: str, wcu: int) -> int:
    if item_count > 200000 or size_bytes > 200000000:
        total_segments = 16
    elif item_count > 50000 or size_bytes > 50000000:
        total_segments = 8
    elif item_count > 5000 or size_bytes > 5000000:
        total_segments = 4
    else:
        total_segments = 1
        
    if billing_mode == "PROVISIONED" and wcu > 0:
        if wcu < 10:
            total_segments = min(total_segments, 2)
            logger.info("Target table runs in Provisioned mode with low capacity (WCU: %d). "
                        "Capping parallel workers to %d to avoid heavy write throttling.", wcu, total_segments)
        elif wcu < 50:
            total_segments = min(total_segments, 4)
            logger.info("Target table runs in Provisioned mode with moderate capacity (WCU: %d). "
                        "Capping parallel workers to %d to maintain write stability.", wcu, total_segments)
            
    return total_segments


def run_migration(
    src_profile: str,
    src_region: str,
    src_table: str,
    tgt_profile: str,
    tgt_region: str,
    tgt_table: str,
    billing_mode: str,
    wcu: int,
    item_count: int,
    size_bytes: int,
    checkpoint_mgr: CheckpointManager,
    resume: bool
) -> None:
    checkpoint_state = checkpoint_mgr.load()
    resumed_segments = checkpoint_state.get("segments", {}) if resume else {}
    
    if resume:
        total_segments = checkpoint_state.get("total_segments", 1)
        logger.info("Resuming migration using %d parallel segments from checkpoint.", total_segments)
    else:
        total_segments = determine_concurrency(item_count, size_bytes, billing_mode, wcu)
        logger.info("Optimal concurrency calculated: Spawning %d parallel migration worker(s)...", total_segments)
        
    progress_tracker = ProgressTracker(item_count)
    start_time = time.time()
    
    futures = []
    total_copied = 0
    
    with ThreadPoolExecutor(max_workers=total_segments) as executor:
        for seg_id in range(total_segments):
            seg_checkpoint = resumed_segments.get(str(seg_id))
            
            if resume and seg_checkpoint == "COMPLETE":
                logger.info("[Segment %d/%d] Already completed in previous run. Skipping.", seg_id + 1, total_segments)
                continue
                
            initial_key = seg_checkpoint if isinstance(seg_checkpoint, dict) else None
            
            futures.append(
                executor.submit(
                    migrate_segment,
                    src_profile,
                    src_region,
                    src_table,
                    tgt_profile,
                    tgt_region,
                    tgt_table,
                    seg_id,
                    total_segments,
                    progress_tracker,
                    checkpoint_mgr,
                    initial_key
                )
            )
            
        for future in as_completed(futures):
            try:
                total_copied += future.result()
            except Exception as e:
                logger.error("Migration failed in a parallel thread. Aborting execution.")
                sys.exit(1)
                
    elapsed = time.time() - start_time
    logger.info("Successfully copied items to target table '%s' in %.2f seconds.", tgt_table, elapsed)
    
    # Success cleanup
    checkpoint_mgr.delete()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DynamoDB Auto-Optimizing Cross-Account Replication Utility")
    parser.add_argument("--src-profile", type=str, help="AWS CLI profile for the source account")
    parser.add_argument("--src-region", type=str, default="us-east-1", help="AWS region for the source table")
    parser.add_argument("--src-table", type=str, help="Source DynamoDB Table Name")
    parser.add_argument("--tgt-profile", type=str, help="AWS CLI profile for the target account")
    parser.add_argument("--tgt-region", type=str, default="us-east-1", help="AWS region for the target table")
    parser.add_argument("--tgt-table", type=str, help="Name of target table to create or migrate into")
    parser.add_argument("--yes", action="store_true", help="Skip the final confirmation prompt")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    return parser.parse_args()


def run_interactive_wizard() -> Tuple[str, str, str, str, str, bool, str]:
    print("=========================================================")
    print("   * DynamoDB Cross-Account Auto-Migration Wizard *")
    print("=========================================================")
    
    print("\n--- [1] Source AWS Configuration ---")
    src_profile = choose_aws_profile("Source AWS Profile")
    src_region = get_input("Source AWS Region", default="us-east-1")
    
    try:
        src_session = boto3.Session(profile_name=src_profile, region_name=src_region)
    except Exception as e:
        logger.error("Failed to initialize Source AWS Session: %s", e)
        sys.exit(1)
        
    src_table = choose_ddb_table(src_session, src_region, "Source DynamoDB Table Name")
    if not src_table:
        logger.error("Source Table Name is required.")
        sys.exit(1)
        
    print("\n--- [2] Target AWS Configuration ---")
    tgt_profile = choose_aws_profile("Target AWS Profile")
    tgt_region = get_input("Target AWS Region", default=src_region)
    
    try:
        tgt_session = boto3.Session(profile_name=tgt_profile, region_name=tgt_region)
        tgt_client = tgt_session.client("dynamodb")
    except Exception as e:
        logger.error("Failed to initialize Target AWS Session: %s", e)
        sys.exit(1)
        
    table_action_choice = get_interactive_choice(
        [
            "Recreate table with same name as source",
            "Recreate table with a new custom name",
            "Migrate items into an existing target table (no schema replication)"
        ],
        "Configure Target DynamoDB Action:"
    )
    
    recreate_table = True
    tgt_table = ""
    
    if table_action_choice == "Recreate table with same name as source":
        tgt_table = src_table
    elif table_action_choice == "Recreate table with a new custom name":
        tgt_table = get_input("Enter target Table Name")
        if not tgt_table:
            logger.error("Target Table Name is required.")
            sys.exit(1)
    else:
        recreate_table = False
        tgt_table = choose_ddb_table(tgt_session, tgt_region, "Target DynamoDB Table Name")
        if not tgt_table:
            logger.error("Target Table Name is required.")
            sys.exit(1)
            
    if recreate_table:
        try:
            target_tables_resp = tgt_client.list_tables(Limit=100)
            existing_tables = target_tables_resp.get("TableNames", [])
            if tgt_table in existing_tables:
                print(f"\n⚠️  WARNING: A DynamoDB table named '{tgt_table}' already exists in the target account.")
                use_existing = get_input("Would you like to migrate into the existing table instead of recreating? (y/n)", default="y")
                if use_existing.lower() == "y":
                    recreate_table = False
                    print(f"Switching target to use existing table: {tgt_table}")
        except Exception as e:
            logger.warning("Could not check existing target tables list: %s", e)
            
    return src_profile, src_region, src_table, tgt_profile, tgt_region, recreate_table, tgt_table


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)
    
    checkpoint_mgr = CheckpointManager()
    checkpoint_state = checkpoint_mgr.load()
    
    resume = False
    is_automated = bool(args.src_profile and args.src_table and args.tgt_profile)
    
    if is_automated:
        src_profile = args.src_profile
        src_region = args.src_region
        src_table = args.src_table
        tgt_profile = args.tgt_profile
        tgt_region = args.tgt_region
        tgt_table = args.tgt_table or src_table
        recreate_table = True
        
        # Check if automated run matches checkpoint criteria to auto-resume
        if (checkpoint_state and checkpoint_state.get("src_table") == src_table 
                and checkpoint_state.get("tgt_table") == tgt_table):
            logger.info("Found interrupted migration checkpoint in automated mode. Resuming...")
            resume = True
            recreate_table = False
    else:
        # Check for checkpoint resume (Wizard Mode)
        if checkpoint_state:
            print("\n=========================================================")
            print("         ⚠️  INTERRUPTED MIGRATION DETECTED")
            print("=========================================================")
            print(f" Source Table:  {checkpoint_state.get('src_table')}")
            print(f" Target Table:  {checkpoint_state.get('tgt_table')}")
            print("=========================================================")
            
            res_opt = get_input("Would you like to resume this migration from the last checkpoint? (y/n)", default="y")
            if res_opt.lower() == "y":
                resume = True
                recreate_table = False
                src_profile = choose_aws_profile("Verify Source AWS Profile")
                src_region = get_input("Verify Source AWS Region", default="us-east-1")
                src_table = checkpoint_state.get("src_table", "")
                
                tgt_profile = choose_aws_profile("Verify Target AWS Profile")
                tgt_region = get_input("Verify Target AWS Region", default=src_region)
                tgt_table = checkpoint_state.get("tgt_table", "")
            else:
                checkpoint_mgr.delete()
                
        if not resume:
            (src_profile, src_region, src_table, tgt_profile, tgt_region, 
             recreate_table, tgt_table) = run_interactive_wizard()
             
    try:
        src_session = boto3.Session(profile_name=src_profile, region_name=src_region)
        src_client = src_session.client("dynamodb")
        
        tgt_session = boto3.Session(profile_name=tgt_profile, region_name=tgt_region)
        tgt_client = tgt_session.client("dynamodb")
    except Exception as e:
        logger.error("Failed to initialize AWS sessions or clients: %s", e)
        sys.exit(1)
        
    try:
        src_table_desc = src_client.describe_table(TableName=src_table)
    except ClientError as e:
        logger.error("Failed to describe source table '%s': %s", src_table, e.response["Error"]["Message"])
        sys.exit(1)
        
    # Sizing parameters
    item_count = src_table_desc["Table"].get("ItemCount", 0)
    size_bytes = src_table_desc["Table"].get("TableSizeBytes", 0)
        
    print("\n=========================================================")
    print("                  DYNAMODB REPLICATION SUMMARY")
    print("=========================================================")
    print(f" Source Account Profile:  {src_profile} (Region: {src_region})")
    print(f" Source Table Name:       {src_table} ({item_count} approx items, {size_bytes / 1024 / 1024:.2f} MB)")
    print(f" Target Account Profile:  {tgt_profile} (Region: {tgt_region})")
    print(f" Target Action:           {'RESUME INTERRUPTED MIGRATION' if resume else ('RECREATE TABLE & COPY DATA' if recreate_table else 'COPY DATA INTO EXISTING TABLE')}")
    print(f" Target Table Name:       {tgt_table}")
    print("=========================================================")
    
    if not args.yes:
        confirm = get_input("Do you want to proceed with the replication? (y/n)", default="n")
        if confirm.lower() != "y":
            logger.info("Replication cancelled.")
            sys.exit(0)
            
    billing_mode = "PROVISIONED"
    wcu = 1
    
    if recreate_table:
        billing_mode, wcu = replicate_table_schema(tgt_client, src_table_desc, tgt_table)
        
        # Wait for table to be active in target account
        logger.info("Waiting for target table '%s' to become ACTIVE...", tgt_table)
        try:
            waiter = tgt_client.get_waiter("table_exists")
            waiter.wait(
                TableName=tgt_table,
                WaiterConfig={"Delay": 5, "MaxAttempts": 20}
            )
            logger.info("Target table is active and ready for replication.")
        except Exception as e:
            logger.error("Timed out waiting for target table to become active: %s", e)
            sys.exit(1)
    else:
        # Check target capacity settings for thread safety
        try:
            tgt_desc = tgt_client.describe_table(TableName=tgt_table)["Table"]
            billing_mode, provisioned_throughput = parse_billing_and_capacity(tgt_desc)
            wcu = provisioned_throughput["WriteCapacityUnits"] if provisioned_throughput else 0
        except Exception as e:
            logger.warning("Could not read target table configurations, using default concurrency. Error: %s", e)
            
    run_migration(
        src_profile=src_profile,
        src_region=src_region,
        src_table=src_table,
        tgt_profile=tgt_profile,
        tgt_region=tgt_region,
        tgt_table=tgt_table,
        billing_mode=billing_mode,
        wcu=wcu,
        item_count=item_count,
        size_bytes=size_bytes,
        checkpoint_mgr=checkpoint_mgr,
        resume=resume
    )
    
    print("\n=========================================================")
    print("DynamoDB Cross-Account Replication Completed Successfully!")
    print("=========================================================")
    print(f"Table replicated: {tgt_table}")
    print("=========================================================")


if __name__ == "__main__":
    main()
