# import matplotlib
# matplotlib.use('Agg')
# import traceback
# import logging
# import os
# import sys
# import tempfile
# import time
# from app.config_loader import load_config
# from app.s3_handler import S3Handler
# from app.chatgpt_analyzer import run_yolo_analysis
# from app.file_uploader import FileUploader
# from app.db_handler import initialize_db_connection, close_db_connection
# from app.visicooler import run_visicooler_analysis, check_visibilitydetails_schema
# from cap_pipeline_runner import run_cap_pipeline, reclassify_low_detection_impure, run_planogram_pipeline
# from shelf_sequence_checker import run_shelf_sequence_check, run_shelf_sequence_check_planogram
# from visicooler_presence_detector import run_visicooler_presence_analysis
# from planogram_detector import run_planogram_detection

# # ── Telegram bot ───────────────────────────────────────────────────────────────
# from bot_notifier import (
#     PipelineHeartbeat, PipelineQABot,
#     notify_pipeline_start, notify_batch_start, notify_batch_complete,
#     notify_batch_failed, notify_download_complete, notify_yolo_complete,
#     notify_db_upload_complete, notify_pipeline_complete, notify_pipeline_error,
#     notify_stale_reset, notify_cap_pipeline,
# )

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('outputs/pipeline.log'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)

# # Reset stale assignments after 60 minutes
# STALE_TIMEOUT_MINUTES = 60

# # ── Shared pipeline state (read by heartbeat + Q&A bot) ───────────────────────
# _pipeline_state = {
#     "pod_id": "unknown",
#     "iterationid": 0,
#     "stagingid": 0,
#     "batch_number": 0,
#     "stores_done": 0,
#     "images_done": 0,
#     "remaining_stores": "?",
#     "current_stage": "Initialising",
#     "db_rows_inserted": 0,
#     "recent_errors": [],
# }

# def _update_state(**kwargs):
#     _pipeline_state.update(kwargs)

# def _get_state():
#     return dict(_pipeline_state)

# def _record_error(msg: str):
#     _pipeline_state["recent_errors"].append(f"[{time.strftime('%H:%M:%S')}] {msg}")
#     if len(_pipeline_state["recent_errors"]) > 20:
#         _pipeline_state["recent_errors"] = _pipeline_state["recent_errors"][-20:]


# # ── DB helpers ─────────────────────────────────────────────────────────────────
# def get_unprocessed_store_count(conn):
#     """Count distinct unprocessed stores."""
#     try:
#         cur = conn.cursor()
#         cur.execute("""
#             SELECT COUNT(DISTINCT storeid) 
#             FROM orgi.fileupload 
#             WHERE (processed_flag IN ('P', '0.0') OR processed_flag IS NULL)
#               AND storeid IS NOT NULL
#         """)
#         count = cur.fetchone()[0]
#         cur.close()
#         return count
#     except Exception as e:
#         logger.error(f"Failed to get unprocessed store count: {e}")
#         return 0


# def reset_stale_batches(conn, stale_timeout_minutes):
#     """Reset stores stuck in 'I' status beyond timeout period."""
#     try:
#         cur = conn.cursor()
#         cur.execute("""
#             UPDATE orgi.fileupload
#             SET processed_flag = 'P', podid = NULL
#             WHERE processed_flag = 'I' 
#               AND uploadtimestamp < NOW() - INTERVAL '%s minutes'
#         """ % stale_timeout_minutes)

#         reset_count = cur.rowcount
#         conn.commit()
#         cur.close()

#         if reset_count > 0:
#             logger.warning(f"Reset {reset_count} stale files (stuck >{stale_timeout_minutes}m)")
#             notify_stale_reset(reset_count)

#         return reset_count
#     except Exception as e:
#         logger.error(f"Failed to reset stale batches: {e}")
#         conn.rollback()
#         return 0


# def assign_stores_to_pod(conn, store_count, pod_id):
#     """Updates processed_flag from 'N' to 'I' and sets podid."""
#     try:
#         cur = conn.cursor()

#         cur.execute("""
#             SELECT DISTINCT storeid
#             FROM orgi.fileupload
#             WHERE (processed_flag IN ('P', '0.0') OR processed_flag IS NULL)
#               AND storeid IS NOT NULL
#             ORDER BY storeid
#             LIMIT %s
#         """, (store_count,))

#         selected_stores = [row[0] for row in cur.fetchall()]

#         if not selected_stores:
#             cur.close()
#             return 0, 0

#         cur.execute("""
#             UPDATE orgi.fileupload
#             SET processed_flag = 'I', podid = %s
#             WHERE storeid = ANY(%s)
#               AND (processed_flag IN ('P', '0.0') OR processed_flag IS NULL)
#         """, (pod_id, selected_stores))

#         assigned_files = cur.rowcount
#         assigned_stores = len(selected_stores)

#         conn.commit()
#         cur.close()

#         logger.info(f"Assigned {assigned_stores} stores ({assigned_files} images) to {pod_id}")
#         return assigned_stores, assigned_files

#     except Exception as e:
#         logger.error(f"Failed to assign stores to pod: {e}")
#         conn.rollback()
#         return 0, 0


# def get_or_create_iterationid(conn):
#     try:
#         cur = conn.cursor()
#         cur.execute("SELECT COALESCE(MAX(iteration_id), 0) FROM temp.cap_prediction_temp")
#         max_iteration = cur.fetchone()[0]
#         iterationid = max_iteration + 1
#         cur.close()
#         logger.info(f"Using iterationid: {iterationid} for this pipeline run")
#         return iterationid
#     except Exception as e:
#         logger.error(f"Failed to get iterationid: {e}")
#         return 1


# def execute_models(pod_id, iterationid, stagingid):
#     """
#     Run the processing pipeline for stores assigned to this pod.
#     stagingid is shared across all batches in this run.
#     """
#     conn = None
#     cur = None
#     try:
#         config = load_config('config.json')
#         ollama_config = config['ollama_config']
#         s3_config = config['s3_config']
#         db_config = config['db_config']
#         visicooler_config = config['visicooler_config']

#         conn, cur = initialize_db_connection(db_config)

#         if not check_visibilitydetails_schema(cur):
#             msg = "Schema validation failed for orgi.visibilitydetails"
#             logger.error(msg)
#             _record_error(msg)
#             notify_pipeline_error("Schema Validation", msg, pod_id)
#             return False

#         s3_handler = S3Handler(s3_config, db_config)

#         with tempfile.TemporaryDirectory() as temp_dir:
#             logger.info(f"Created temp directory: {temp_dir}")

#             # ── Download images ───────────────────────────────────────────────
#             _update_state(current_stage="Downloading Images")
#             logger.info(f"Downloading images for {pod_id}...")
#             image_paths, failed_files = s3_handler.download_images_from_s3(temp_dir, pod_id)
#             notify_download_complete(
#                 total=len(image_paths) + len(failed_files),
#                 failed=len(failed_files)
#             )

#             if not image_paths:
#                 msg = f"No images downloaded for {pod_id}"
#                 logger.warning(msg)
#                 _record_error(msg)
#                 return False

#             unique_stores = len(set(img[5] for img in image_paths if img[5]))
#             logger.info(f"Processing {unique_stores} stores ({len(image_paths)} images)")

#             if failed_files:
#                 logger.warning(f"Failed downloads: {len(failed_files)}")

#             # ── Get cyclecountid ──────────────────────────────────────────────
#             try:
#                 cur.execute("""
#                     SELECT GREATEST(
#                         COALESCE((SELECT MAX(cyclecountid) FROM orgi.visibilitydetails), 0),
#                         COALESCE((SELECT MAX(stagingid) FROM orgi.visibilityitemsstaging), 0)
#                     ) AS max_cycle
#                 """)
#                 row = cur.fetchone()
#                 max_cycle = int(row[0]) if row and row[0] is not None else 0
#                 cyclecountid = max_cycle + 1
#                 logger.info(f"Using cyclecountid: {cyclecountid}")
#             except Exception as e:
#                 logger.error(f"Failed to compute cyclecountid: {e}")
#                 cyclecountid = 1

#             # ── Visicooler analysis ───────────────────────────────────────────
#             _update_state(current_stage="Visicooler Analysis")
#             logger.info(f"Running visicooler analysis (iterationid: {iterationid})...")
#             try:
#                 visicooler_records = run_visicooler_analysis(
#                     image_paths=image_paths,
#                     config=config,
#                     s3_handler=s3_handler,
#                     conn=conn,
#                     cur=cur,
#                     output_folder_path=visicooler_config['output_folder_path'],
#                     cyclecountid=cyclecountid,
#                     iterationid=iterationid
#                 )
#                 logger.info(f"Visicooler analysis complete: {len(visicooler_records)} records")
#             except Exception as e:
#                 msg = f"Visicooler analysis failed: {e}"
#                 logger.error(msg)
#                 _record_error(msg)
#                 notify_pipeline_error("Visicooler Analysis", str(e), pod_id)
#                 visicooler_records = []

#             # Connection closed by visicooler analysis
#             conn = None

#             # ── Visicooler Presence Detection (subcategory 601) ────────────────
#             _update_state(current_stage="Visicooler Presence Detection")
#             logger.info("Running visicooler-presence detection on subcategory 601 images...")
#             try:
#                 visicooler_model_path = visicooler_config.get(
#                     'visicooler_model_path', visicooler_config.get('model_path')
#                 )
#                 presence_results, presence_csv = run_visicooler_presence_analysis(
#                     image_paths=image_paths,
#                     db_config=db_config,
#                     stagingid=stagingid,
#                     visicooler_model_path=visicooler_model_path,
#                     conf_threshold=float(visicooler_config.get('visicooler_presence_conf_threshold', 0.1)),
#                     s3_handler=s3_handler,
#                     s3_annotated_folder=f"{ollama_config['output_s3_folder']}Visicooler_{cyclecountid}",
#                     output_csv=visicooler_config.get('visicooler_presence_output_csv'),
#                     subcategory_id=int(visicooler_config.get('visicooler_presence_subcategory_id', 601)),
#                     fallback_model_path=visicooler_config.get(
#                         'visicooler_presence_fallback_model_path', 'yolov8x.pt'
#                     ),
#                     fallback_conf_threshold=float(
#                         visicooler_config.get(
#                             'visicooler_presence_fallback_conf_threshold',
#                             visicooler_config.get('visicooler_presence_conf_threshold', 0.1)
#                         )
#                     ),
#                 )
#                 presence_count = len(presence_results) if presence_results else 0
#                 logger.info(f"Visicooler-presence detection complete: {presence_count} rows inserted")
#                 _update_state(db_rows_inserted=_pipeline_state["db_rows_inserted"] + presence_count)
#             except Exception as e:
#                 msg = f"Visicooler-presence detection failed: {e}"
#                 logger.error(msg)
#                 logger.error(traceback.format_exc())
#                 _record_error(msg)
#                 notify_pipeline_error("Visicooler Presence Detection", str(e), pod_id)

#             # ── Planogram Detection (subcategory 603) ───────────────────────────
#             _update_state(current_stage="Planogram Detection")
#             logger.info("Running planogram SKU-only detection on subcategory 603 images...")
#             try:
#                 planogram_rows = run_planogram_detection(
#                     image_paths=image_paths,
#                     config=config,
#                     s3_handler=s3_handler,
#                     db_config=db_config,
#                     cyclecountid=cyclecountid,
#                     iterationid=iterationid,
#                 )
#                 logger.info(f"Planogram detection complete: {planogram_rows} rows inserted")
#             except Exception as e:
#                 msg = f"Planogram detection failed: {e}"
#                 logger.error(msg)
#                 logger.error(traceback.format_exc())
#                 _record_error(msg)
#                 notify_pipeline_error("Planogram Detection", str(e), pod_id)

#             # ── YOLO analysis ─────────────────────────────────────────────────
#             _update_state(current_stage="YOLO Analysis")
#             logger.info("Running YOLO analysis...")
#             try:
#                 ollama_cfg = config['ollama_config']
#                 yolo_results, yolo_csv = run_yolo_analysis(
#                     image_paths=image_paths,
#                     image_folder=temp_dir,
#                     output_csv=ollama_cfg.get('output_csv', 'outputs/yolo_analysis_results.csv'),
#                     config_path='config.json',
#                     class_ids_path=ollama_cfg['class_ids_path'],
#                     s3_handler=s3_handler,
#                     s3_annotated_folder=f"{ollama_cfg['output_s3_folder']}VisibleItem_{cyclecountid}",
#                     db_config=db_config,
#                     cyclecountid=cyclecountid,
#                     stagingid=stagingid
#                 )
#                 yolo_count = len(yolo_results) if yolo_results else 0
#                 notify_yolo_complete(yolo_count)
#                 _update_state(db_rows_inserted=_pipeline_state["db_rows_inserted"] + yolo_count)
#             except Exception as e:
#                 msg = f"YOLO analysis failed: {e}"
#                 logger.error(msg)
#                 _record_error(msg)
#                 notify_pipeline_error("YOLO Analysis", str(e), pod_id)
#                 yolo_results = None

#             # ── DB upload ─────────────────────────────────────────────────────
#             _update_state(current_stage="DB Upload")
#             if yolo_results:
#                 notify_db_upload_complete(stagingid, len(yolo_results))
#             else:
#                 logger.warning("No YOLO CSV to upload")

#             # ── Update processed_flag to 'Y' ──────────────────────────────────
#             _update_state(current_stage="Finalising")
#             logger.info("Updating processed_flag in database")
#             try:
#                 conn, cur = initialize_db_connection(db_config)
#                 file_uploader = FileUploader(None)
#                 failed_updates = file_uploader.update_processed_flag(conn, image_paths)
#                 if failed_updates:
#                     logger.warning(f"Failed to update {len(failed_updates)} files")
#                 else:
#                     logger.info(f"Successfully updated {len(image_paths)} files to processed")
#             except Exception as e:
#                 msg = f"Update processed_flag failed: {e}"
#                 logger.error(msg)
#                 _record_error(msg)

#             logger.info("=" * 60)
#             logger.info(f"BATCH COMPLETE - Pod: {pod_id} (Iteration: {iterationid}, Staging: {stagingid})")
#             logger.info(f"  Stores processed    : {unique_stores}")
#             logger.info(f"  Images processed    : {len(image_paths)}")
#             logger.info(f"  Visicooler records  : {len(visicooler_records)}")
#             logger.info(f"  YOLO records        : {len(yolo_results) if yolo_results else 0}")
#             logger.info("=" * 60)

#             # Update cumulative state
#             _update_state(
#                 stores_done=_pipeline_state["stores_done"] + unique_stores,
#                 images_done=_pipeline_state["images_done"] + len(image_paths),
#             )

#         return True, unique_stores, len(image_paths), len(visicooler_records), len(yolo_results) if yolo_results else 0

#     except Exception as e:
#         logger.error(f"Error in execute_models: {e}")
#         logger.error(traceback.format_exc())
#         _record_error(str(e))
#         if conn is not None:
#             try:
#                 conn.rollback()
#             except Exception:
#                 pass
#         return False, 0, 0, 0, 0
#     finally:
#         if conn is not None:
#             try:
#                 close_db_connection(conn, cur)
#             except Exception:
#                 pass


# def main():
#     if len(sys.argv) < 2:
#         logger.error("Usage: python main.py <pod-id> [max-batch-size]")
#         logger.error("Example: python main.py pod-1")
#         logger.error("Example: python main.py pod-1 50")
#         sys.exit(1)

#     pod_id = sys.argv[1]

#     # Optional 2nd CLI arg caps how many stores go into a single batch.
#     # Defaults to 50. The ACTUAL batch size used each iteration is always
#     # min(max_batch_size, unprocessed_stores) — recomputed every loop, not
#     # just once — so batch sizing tracks however many stores are currently
#     # waiting instead of locking onto whatever was true when the run started.
#     try:
#         max_batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50
#         if max_batch_size < 1:
#             raise ValueError
#     except ValueError:
#         logger.error(f"Invalid max-batch-size '{sys.argv[2]}', must be a positive integer")
#         sys.exit(1)

#     logger.info(f"Starting pipeline for {pod_id} (max batch size: {max_batch_size})")

#     config = load_config('config.json')
#     db_config = config['db_config']

#     # Generate unique IDs once for the entire pipeline run
#     conn, cur = initialize_db_connection(db_config)
#     iterationid = get_or_create_iterationid(conn)

#     cur.execute("SELECT MAX(stagingid) FROM orgi.visibilityitemsstaging")
#     result = cur.fetchone()
#     stagingid = (result[0] if result[0] is not None else 0) + 1
#     logger.info(f"Using stagingid: {stagingid} for this entire run")

#     # ── Initial state for bot ──────────────────────────────────────────────────
#     _update_state(
#         pod_id=pod_id,
#         iterationid=iterationid,
#         stagingid=stagingid,
#         current_stage="Clearing Temp Tables",
#     )

#     # Clear temp tables once before batch loop
#     logger.info("=" * 60)
#     logger.info(f"CLEARING TEMP TABLES (iteration {iterationid})")
#     logger.info("=" * 60)
#     try:
#         cur.execute("DELETE FROM temp.sku_prediction_temp WHERE iteration_id = %s", (iterationid,))
#         deleted_sku = cur.rowcount
#         cur.execute("DELETE FROM temp.cap_prediction_temp WHERE iteration_id = %s", (iterationid,))
#         deleted_cap = cur.rowcount
#         cur.execute("""
#             SELECT 1 FROM information_schema.tables
#             WHERE table_schema = 'temp' AND table_name = 'structural_count_temp'
#         """)
#         if cur.fetchone() is not None:
#             cur.execute(
#                 "DELETE FROM temp.structural_count_temp WHERE iteration_id = %s",
#                 (iterationid,)
#             )
#         cur.execute("""
#             SELECT 1 FROM information_schema.tables
#             WHERE table_schema = 'temp' AND table_name = 'sku_planogram_temp'
#         """)
#         deleted_planogram = 0
#         if cur.fetchone() is not None:
#             cur.execute(
#                 "DELETE FROM temp.sku_planogram_temp WHERE iteration_id = %s",
#                 (iterationid,)
#             )
#             deleted_planogram = cur.rowcount
#         conn.commit()
#         logger.info(
#             f"Cleared {deleted_sku} SKU records, {deleted_cap} cap records, "
#             f"{deleted_planogram} planogram SKU records"
#         )
#         logger.info("Temp tables ready — batches will APPEND data")
#     except Exception as e:
#         logger.error(f"Failed to clear temp tables: {e}")
#         conn.rollback()

#     # Get initial unprocessed store count for the start notification
#     unprocessed_stores_initial = get_unprocessed_store_count(conn)
#     close_db_connection(conn, cur)

#     # ── Start Telegram bot threads ─────────────────────────────────────────────
#     heartbeat = PipelineHeartbeat(get_status_fn=_get_state)
#     qa_bot    = PipelineQABot(get_status_fn=_get_state)
#     heartbeat.start()
#     qa_bot.start()

#     # ── Notify pipeline start ──────────────────────────────────────────────────
#     notify_pipeline_start(pod_id, iterationid, stagingid, unprocessed_stores_initial)

#     logger.info("=" * 60)
#     logger.info("Pipeline Run Configuration:")
#     logger.info(f"  Iteration ID : {iterationid} (shared across all batches)")
#     logger.info(f"  Staging ID   : {stagingid} (shared across all batches)")
#     logger.info(f"  Vision model : YOLO activation only (no LLM)")
#     logger.info("=" * 60)

#     batch_number = 0
#     cap_status   = "not_run"

#     try:
#         while True:
#             try:
#                 conn, cur = initialize_db_connection(db_config)

#                 reset_stale_batches(conn, STALE_TIMEOUT_MINUTES)

#                 unprocessed_stores = get_unprocessed_store_count(conn)
#                 _update_state(remaining_stores=unprocessed_stores)

#                 if unprocessed_stores == 0:
#                     logger.info("=" * 60)
#                     logger.info("All stores processed successfully")
#                     logger.info(f"  Iteration ID  : {iterationid}")
#                     logger.info(f"  Staging ID    : {stagingid}")
#                     logger.info(f"  Total batches : {batch_number}")
#                     logger.info("=" * 60)
#                     close_db_connection(conn, cur)

#                     # ── CAP Post-Processing Pipeline ──────────────────────────
#                     _update_state(current_stage="CAP Post-Processing")
#                     logger.info("=" * 60)
#                     logger.info("Starting CAP prediction post-processing pipeline...")
#                     logger.info("=" * 60)
#                     try:
#                         cap_result = run_cap_pipeline(
#                             db_config=db_config,
#                             iteration_id=iterationid,
#                             staging_id=stagingid,
#                         )
#                         if cap_result.overall_status != "success":
#                             cap_status = cap_result.overall_status
#                             logger.error("CAP pipeline finished with errors")
#                             notify_cap_pipeline(cap_status, cap_result.total_duration_ms)
#                         else:
#                             cap_status = "success"
#                             logger.info(
#                                 f"CAP pipeline completed successfully "
#                                 f"in {cap_result.total_duration_ms:.0f} ms."
#                             )
#                             notify_cap_pipeline("success", cap_result.total_duration_ms)
#                     except Exception as e:
#                         cap_status = "error"
#                         msg = f"CAP pipeline exception: {e}"
#                         logger.error(msg)
#                         logger.error(traceback.format_exc())
#                         _record_error(msg)
#                         notify_pipeline_error("CAP Pipeline", str(e), pod_id)

#                     # ── Planogram Pipeline (subcategory 603) ──────────────────
#                     # Runs AFTER run_cap_pipeline for this same iterationid:
#                     # its iterationtranid-offset logic reads
#                     # orgi.coolermetricsmaster's current max tranid to keep
#                     # planogram tranids from colliding with 605's in the
#                     # shared orgi.shelfsequencecompliance table downstream.
#                     _update_state(current_stage="Planogram Pipeline")
#                     logger.info("=" * 60)
#                     logger.info("Starting planogram post-processing pipeline...")
#                     logger.info("=" * 60)
#                     try:
#                         planogram_result = run_planogram_pipeline(
#                             db_config=db_config,
#                             iteration_id=iterationid,
#                         )
#                         if planogram_result.overall_status != "success":
#                             logger.error("Planogram pipeline finished with errors")
#                             notify_pipeline_error(
#                                 "Planogram Pipeline", "One or more steps failed", pod_id
#                             )
#                         else:
#                             logger.info(
#                                 f"Planogram pipeline completed successfully "
#                                 f"in {planogram_result.total_duration_ms:.0f} ms."
#                             )
#                     except Exception as e:
#                         msg = f"Planogram pipeline exception: {e}"
#                         logger.error(msg)
#                         logger.error(traceback.format_exc())
#                         _record_error(msg)
#                         notify_pipeline_error("Planogram Pipeline", str(e), pod_id)

#                     # ── Shelf-Sequence Compliance Check ───────────────────────
#                     _update_state(current_stage="Shelf Sequence Check")
#                     logger.info("=" * 60)
#                     logger.info(f"Running shelf-sequence check for iteration {iterationid}...")
#                     logger.info("=" * 60)
#                     try:
#                         shelf_results = run_shelf_sequence_check(
#                             db_config=db_config,
#                             iterationid=iterationid,
#                         )
#                         shelf_passed = sum(1 for r in shelf_results if r[3] == 'Y')
#                         logger.info(
#                             f"Shelf-sequence check done: {shelf_passed}/{len(shelf_results)} "
#                             f"images compliant (iteration {iterationid})"
#                         )

#                         # Reclassify low-detection-count IMPURE rows back to
#                         # PURE. Must run AFTER run_shelf_sequence_check(),
#                         # since that's what populates purity_status and
#                         # dino_nonbeverage_detections for this iteration in
#                         # the first place — running it any earlier (e.g. as
#                         # a step inside run_cap_pipeline, which executes
#                         # before this check) always matched 0 rows because
#                         # the data it depends on didn't exist yet.
#                         reclassified = reclassify_low_detection_impure(
#                             db_config=db_config,
#                             iteration_id=iterationid,
#                         )
#                         logger.info(
#                             f"Reclassified {reclassified} low-detection-count "
#                             f"IMPURE row(s) back to PURE (iteration {iterationid})"
#                         )
#                     except Exception as e:
#                         msg = f"Shelf-sequence check exception: {e}"
#                         logger.error(msg)
#                         logger.error(traceback.format_exc())
#                         _record_error(msg)
#                         notify_pipeline_error("Shelf Sequence Check", str(e), pod_id)

#                     # ── Planogram Shelf-Sequence Compliance Check ─────────────
#                     _update_state(current_stage="Planogram Shelf Sequence Check")
#                     logger.info("=" * 60)
#                     logger.info(f"Running planogram shelf-sequence check for iteration {iterationid}...")
#                     logger.info("=" * 60)
#                     try:
#                         planogram_shelf_results = run_shelf_sequence_check_planogram(
#                             db_config=db_config,
#                             iterationid=iterationid,
#                         )
#                         planogram_shelf_passed = sum(
#                             1 for r in planogram_shelf_results if r[3] == 'Y'
#                         )
#                         logger.info(
#                             f"Planogram shelf-sequence check done: "
#                             f"{planogram_shelf_passed}/{len(planogram_shelf_results)} "
#                             f"images compliant (iteration {iterationid})"
#                         )
#                     except Exception as e:
#                         msg = f"Planogram shelf-sequence check exception: {e}"
#                         logger.error(msg)
#                         logger.error(traceback.format_exc())
#                         _record_error(msg)
#                         notify_pipeline_error("Planogram Shelf Sequence Check", str(e), pod_id)
#                     break

#                 logger.info("=" * 60)
#                 logger.info(f"Unprocessed stores remaining: {unprocessed_stores}")
#                 logger.info("=" * 60)

#                 # Recompute every iteration: if fewer than max_batch_size stores
#                 # are waiting, process all of them (batch = unprocessed_stores);
#                 # once 50+ are waiting, cap the batch at max_batch_size and let
#                 # the remainder (e.g. 1 leftover store) form its own batch next
#                 # time round. This is why we don't ask/cache a batch_size once.
#                 current_batch_size = min(max_batch_size, unprocessed_stores)
#                 logger.info(
#                     f"Batch size for this round: {current_batch_size} "
#                     f"(max {max_batch_size}, {unprocessed_stores} unprocessed)"
#                 )

#                 assigned_stores, assigned_files = assign_stores_to_pod(conn, current_batch_size, pod_id)
#                 close_db_connection(conn, cur)

#                 if assigned_stores == 0:
#                     logger.warning("No stores assigned. Retrying in 10 seconds")
#                     time.sleep(10)
#                     continue

#                 batch_number += 1
#                 _update_state(batch_number=batch_number, current_stage="Processing Batch")
#                 logger.info(
#                     f"Starting batch {batch_number}: "
#                     f"{assigned_stores} stores, {assigned_files} images"
#                 )
#                 notify_batch_start(batch_number, assigned_stores, assigned_files, pod_id)

#                 result = execute_models(pod_id, iterationid, stagingid)

#                 # execute_models now returns (success, stores, images, visicooler, yolo)
#                 if isinstance(result, tuple):
#                     success, b_stores, b_images, b_visicooler, b_yolo = result
#                 else:
#                     success = result
#                     b_stores = b_images = b_visicooler = b_yolo = 0

#                 if not success:
#                     logger.warning(f"Batch {batch_number} failed. Waiting 10 seconds before retry")
#                     notify_batch_failed(batch_number, "execute_models returned False", pod_id)
#                     time.sleep(10)
#                 else:
#                     # Refresh remaining count for the notification
#                     conn2, cur2 = initialize_db_connection(db_config)
#                     remaining_after = get_unprocessed_store_count(conn2)
#                     close_db_connection(conn2, cur2)
#                     _update_state(remaining_stores=remaining_after)

#                     notify_batch_complete(
#                         batch_number, b_stores, b_images,
#                         b_visicooler, b_yolo, remaining_after
#                     )
#                     logger.info(f"Batch {batch_number} completed. Checking for next batch")
#                     time.sleep(5)

#             except KeyboardInterrupt:
#                 raise
#             except Exception as e:
#                 msg = f"Main loop error: {e}"
#                 logger.error(msg)
#                 logger.error(traceback.format_exc())
#                 _record_error(msg)
#                 notify_pipeline_error("Main Loop", str(e), pod_id)
#                 time.sleep(10)

#     except KeyboardInterrupt:
#         logger.info("Pipeline interrupted by user")
#         notify_pipeline_error("User Interrupt", "Pipeline stopped by operator (KeyboardInterrupt)", pod_id)

#     finally:
#         # ── Stop bot threads ───────────────────────────────────────────────────
#         heartbeat.stop()
#         qa_bot.stop()

#         # ── Final notification ─────────────────────────────────────────────────
#         notify_pipeline_complete(pod_id, iterationid, stagingid, batch_number, cap_status)

#     logger.info(f"Pipeline execution completed for {pod_id}")


# if __name__ == "__main__":
#     main()

import matplotlib
matplotlib.use('Agg')
import traceback
import logging
import os
import sys
import tempfile
import time
from app.config_loader import load_config
from app.s3_handler import S3Handler
from app.chatgpt_analyzer import run_yolo_analysis
from app.file_uploader import FileUploader
from app.db_handler import initialize_db_connection, close_db_connection
from app.visicooler import run_visicooler_analysis, check_visibilitydetails_schema
from app.cap_vs_sku import run_reconciliation
from cap_pipeline_runner import run_cap_pipeline, reclassify_low_detection_impure, run_planogram_pipeline
from shelf_sequence_checker import run_shelf_sequence_check, run_shelf_sequence_check_planogram
from visicooler_presence_detector import run_visicooler_presence_analysis
from planogram_detector import run_planogram_detection

# ── Telegram bot ───────────────────────────────────────────────────────────────
from bot_notifier import (
    PipelineHeartbeat, PipelineQABot,
    notify_pipeline_start, notify_batch_start, notify_batch_complete,
    notify_batch_failed, notify_download_complete, notify_yolo_complete,
    notify_db_upload_complete, notify_pipeline_complete, notify_pipeline_error,
    notify_stale_reset, notify_cap_pipeline,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outputs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Reset stale assignments after 60 minutes
STALE_TIMEOUT_MINUTES = 60

# ── Shared pipeline state (read by heartbeat + Q&A bot) ───────────────────────
_pipeline_state = {
    "pod_id": "unknown",
    "iterationid": 0,
    "stagingid": 0,
    "batch_number": 0,
    "stores_done": 0,
    "images_done": 0,
    "remaining_stores": "?",
    "current_stage": "Initialising",
    "db_rows_inserted": 0,
    "recent_errors": [],
}

def _update_state(**kwargs):
    _pipeline_state.update(kwargs)

def _get_state():
    return dict(_pipeline_state)

def _record_error(msg: str):
    _pipeline_state["recent_errors"].append(f"[{time.strftime('%H:%M:%S')}] {msg}")
    if len(_pipeline_state["recent_errors"]) > 20:
        _pipeline_state["recent_errors"] = _pipeline_state["recent_errors"][-20:]


# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_unprocessed_store_count(conn):
    """Count distinct unprocessed stores."""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(DISTINCT storeid) 
            FROM orgi.fileupload 
            WHERE (processed_flag IN ('P', '0.0') OR processed_flag IS NULL)
              AND storeid IS NOT NULL
        """)
        count = cur.fetchone()[0]
        cur.close()
        return count
    except Exception as e:
        logger.error(f"Failed to get unprocessed store count: {e}")
        return 0


def reset_stale_batches(conn, stale_timeout_minutes):
    """Reset stores stuck in 'I' status beyond timeout period."""
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE orgi.fileupload
            SET processed_flag = 'P', podid = NULL
            WHERE processed_flag = 'I' 
              AND uploadtimestamp < NOW() - INTERVAL '%s minutes'
        """ % stale_timeout_minutes)

        reset_count = cur.rowcount
        conn.commit()
        cur.close()

        if reset_count > 0:
            logger.warning(f"Reset {reset_count} stale files (stuck >{stale_timeout_minutes}m)")
            notify_stale_reset(reset_count)

        return reset_count
    except Exception as e:
        logger.error(f"Failed to reset stale batches: {e}")
        conn.rollback()
        return 0


def assign_stores_to_pod(conn, store_count, pod_id):
    """Updates processed_flag from 'N' to 'I' and sets podid."""
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT storeid
            FROM orgi.fileupload
            WHERE (processed_flag IN ('P', '0.0') OR processed_flag IS NULL)
              AND storeid IS NOT NULL
            ORDER BY storeid
            LIMIT %s
        """, (store_count,))

        selected_stores = [row[0] for row in cur.fetchall()]

        if not selected_stores:
            cur.close()
            return 0, 0

        cur.execute("""
            UPDATE orgi.fileupload
            SET processed_flag = 'I', podid = %s
            WHERE storeid = ANY(%s)
              AND (processed_flag IN ('P', '0.0') OR processed_flag IS NULL)
        """, (pod_id, selected_stores))

        assigned_files = cur.rowcount
        assigned_stores = len(selected_stores)

        conn.commit()
        cur.close()

        logger.info(f"Assigned {assigned_stores} stores ({assigned_files} images) to {pod_id}")
        return assigned_stores, assigned_files

    except Exception as e:
        logger.error(f"Failed to assign stores to pod: {e}")
        conn.rollback()
        return 0, 0


def get_or_create_iterationid(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(iteration_id), 0) FROM temp.cap_prediction_temp")
        max_iteration = cur.fetchone()[0]
        iterationid = max_iteration + 1
        cur.close()
        logger.info(f"Using iterationid: {iterationid} for this pipeline run")
        return iterationid
    except Exception as e:
        logger.error(f"Failed to get iterationid: {e}")
        return 1


def execute_models(pod_id, iterationid, stagingid):
    """
    Run the processing pipeline for stores assigned to this pod.
    stagingid is shared across all batches in this run.
    """
    conn = None
    cur = None
    try:
        config = load_config('config.json')
        ollama_config = config['ollama_config']
        s3_config = config['s3_config']
        db_config = config['db_config']
        visicooler_config = config['visicooler_config']

        conn, cur = initialize_db_connection(db_config)

        if not check_visibilitydetails_schema(cur):
            msg = "Schema validation failed for orgi.visibilitydetails"
            logger.error(msg)
            _record_error(msg)
            notify_pipeline_error("Schema Validation", msg, pod_id)
            return False

        s3_handler = S3Handler(s3_config, db_config)

        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"Created temp directory: {temp_dir}")

            # ── Download images ───────────────────────────────────────────────
            _update_state(current_stage="Downloading Images")
            logger.info(f"Downloading images for {pod_id}...")
            image_paths, failed_files = s3_handler.download_images_from_s3(temp_dir, pod_id)
            notify_download_complete(
                total=len(image_paths) + len(failed_files),
                failed=len(failed_files)
            )

            if not image_paths:
                msg = f"No images downloaded for {pod_id}"
                logger.warning(msg)
                _record_error(msg)
                return False

            unique_stores = len(set(img[5] for img in image_paths if img[5]))
            logger.info(f"Processing {unique_stores} stores ({len(image_paths)} images)")

            if failed_files:
                logger.warning(f"Failed downloads: {len(failed_files)}")

            # ── Get cyclecountid ──────────────────────────────────────────────
            try:
                cur.execute("""
                    SELECT GREATEST(
                        COALESCE((SELECT MAX(cyclecountid) FROM orgi.visibilitydetails), 0),
                        COALESCE((SELECT MAX(stagingid) FROM orgi.visibilityitemsstaging), 0)
                    ) AS max_cycle
                """)
                row = cur.fetchone()
                max_cycle = int(row[0]) if row and row[0] is not None else 0
                cyclecountid = max_cycle + 1
                logger.info(f"Using cyclecountid: {cyclecountid}")
            except Exception as e:
                logger.error(f"Failed to compute cyclecountid: {e}")
                cyclecountid = 1

            # ── Visicooler analysis ───────────────────────────────────────────
            _update_state(current_stage="Visicooler Analysis")
            logger.info(f"Running visicooler analysis (iterationid: {iterationid})...")
            try:
                visicooler_records = run_visicooler_analysis(
                    image_paths=image_paths,
                    config=config,
                    s3_handler=s3_handler,
                    conn=conn,
                    cur=cur,
                    output_folder_path=visicooler_config['output_folder_path'],
                    cyclecountid=cyclecountid,
                    iterationid=iterationid
                )
                logger.info(f"Visicooler analysis complete: {len(visicooler_records)} records")
            except Exception as e:
                msg = f"Visicooler analysis failed: {e}"
                logger.error(msg)
                _record_error(msg)
                notify_pipeline_error("Visicooler Analysis", str(e), pod_id)
                visicooler_records = []

            # Connection closed by visicooler analysis
            conn = None

            # ── Visicooler Presence Detection (subcategory 601) ────────────────
            _update_state(current_stage="Visicooler Presence Detection")
            logger.info("Running visicooler-presence detection on subcategory 601 images...")
            try:
                visicooler_model_path = visicooler_config.get(
                    'visicooler_model_path', visicooler_config.get('model_path')
                )
                presence_results, presence_csv = run_visicooler_presence_analysis(
                    image_paths=image_paths,
                    db_config=db_config,
                    stagingid=stagingid,
                    visicooler_model_path=visicooler_model_path,
                    conf_threshold=float(visicooler_config.get('visicooler_presence_conf_threshold', 0.1)),
                    s3_handler=s3_handler,
                    s3_annotated_folder=f"{ollama_config['output_s3_folder']}Visicooler_{cyclecountid}",
                    output_csv=visicooler_config.get('visicooler_presence_output_csv'),
                    subcategory_id=int(visicooler_config.get('visicooler_presence_subcategory_id', 601)),
                    fallback_model_path=visicooler_config.get(
                        'visicooler_presence_fallback_model_path', 'yolov8x.pt'
                    ),
                    fallback_conf_threshold=float(
                        visicooler_config.get(
                            'visicooler_presence_fallback_conf_threshold',
                            visicooler_config.get('visicooler_presence_conf_threshold', 0.1)
                        )
                    ),
                )
                presence_count = len(presence_results) if presence_results else 0
                logger.info(f"Visicooler-presence detection complete: {presence_count} rows inserted")
                _update_state(db_rows_inserted=_pipeline_state["db_rows_inserted"] + presence_count)
            except Exception as e:
                msg = f"Visicooler-presence detection failed: {e}"
                logger.error(msg)
                logger.error(traceback.format_exc())
                _record_error(msg)
                notify_pipeline_error("Visicooler Presence Detection", str(e), pod_id)

            # ── Planogram Detection (subcategory 603) ───────────────────────────
            _update_state(current_stage="Planogram Detection")
            logger.info("Running planogram SKU-only detection on subcategory 603 images...")
            try:
                planogram_rows = run_planogram_detection(
                    image_paths=image_paths,
                    config=config,
                    s3_handler=s3_handler,
                    db_config=db_config,
                    cyclecountid=cyclecountid,
                    iterationid=iterationid,
                )
                logger.info(f"Planogram detection complete: {planogram_rows} rows inserted")
            except Exception as e:
                msg = f"Planogram detection failed: {e}"
                logger.error(msg)
                logger.error(traceback.format_exc())
                _record_error(msg)
                notify_pipeline_error("Planogram Detection", str(e), pod_id)

            # ── YOLO analysis ─────────────────────────────────────────────────
            _update_state(current_stage="YOLO Analysis")
            logger.info("Running YOLO analysis...")
            try:
                ollama_cfg = config['ollama_config']
                yolo_results, yolo_csv = run_yolo_analysis(
                    image_paths=image_paths,
                    image_folder=temp_dir,
                    output_csv=ollama_cfg.get('output_csv', 'outputs/yolo_analysis_results.csv'),
                    config_path='config.json',
                    class_ids_path=ollama_cfg['class_ids_path'],
                    s3_handler=s3_handler,
                    s3_annotated_folder=f"{ollama_cfg['output_s3_folder']}VisibleItem_{cyclecountid}",
                    db_config=db_config,
                    cyclecountid=cyclecountid,
                    stagingid=stagingid
                )
                yolo_count = len(yolo_results) if yolo_results else 0
                notify_yolo_complete(yolo_count)
                _update_state(db_rows_inserted=_pipeline_state["db_rows_inserted"] + yolo_count)
            except Exception as e:
                msg = f"YOLO analysis failed: {e}"
                logger.error(msg)
                _record_error(msg)
                notify_pipeline_error("YOLO Analysis", str(e), pod_id)
                yolo_results = None

            # ── DB upload ─────────────────────────────────────────────────────
            _update_state(current_stage="DB Upload")
            if yolo_results:
                notify_db_upload_complete(stagingid, len(yolo_results))
            else:
                logger.warning("No YOLO CSV to upload")

            # ── Update processed_flag to 'Y' ──────────────────────────────────
            _update_state(current_stage="Finalising")
            logger.info("Updating processed_flag in database")
            try:
                conn, cur = initialize_db_connection(db_config)
                file_uploader = FileUploader(None)
                failed_updates = file_uploader.update_processed_flag(conn, image_paths)
                if failed_updates:
                    logger.warning(f"Failed to update {len(failed_updates)} files")
                else:
                    logger.info(f"Successfully updated {len(image_paths)} files to processed")
            except Exception as e:
                msg = f"Update processed_flag failed: {e}"
                logger.error(msg)
                _record_error(msg)

            logger.info("=" * 60)
            logger.info(f"BATCH COMPLETE - Pod: {pod_id} (Iteration: {iterationid}, Staging: {stagingid})")
            logger.info(f"  Stores processed    : {unique_stores}")
            logger.info(f"  Images processed    : {len(image_paths)}")
            logger.info(f"  Visicooler records  : {len(visicooler_records)}")
            logger.info(f"  YOLO records        : {len(yolo_results) if yolo_results else 0}")
            logger.info("=" * 60)

            # Update cumulative state
            _update_state(
                stores_done=_pipeline_state["stores_done"] + unique_stores,
                images_done=_pipeline_state["images_done"] + len(image_paths),
            )

        return True, unique_stores, len(image_paths), len(visicooler_records), len(yolo_results) if yolo_results else 0

    except Exception as e:
        logger.error(f"Error in execute_models: {e}")
        logger.error(traceback.format_exc())
        _record_error(str(e))
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, 0, 0, 0, 0
    finally:
        if conn is not None:
            try:
                close_db_connection(conn, cur)
            except Exception:
                pass


def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python main.py <pod-id> [max-batch-size]")
        logger.error("Example: python main.py pod-1")
        logger.error("Example: python main.py pod-1 50")
        sys.exit(1)

    pod_id = sys.argv[1]

    # Optional 2nd CLI arg caps how many stores go into a single batch.
    # Defaults to 50. The ACTUAL batch size used each iteration is always
    # min(max_batch_size, unprocessed_stores) — recomputed every loop, not
    # just once — so batch sizing tracks however many stores are currently
    # waiting instead of locking onto whatever was true when the run started.
    try:
        max_batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        if max_batch_size < 1:
            raise ValueError
    except ValueError:
        logger.error(f"Invalid max-batch-size '{sys.argv[2]}', must be a positive integer")
        sys.exit(1)

    logger.info(f"Starting pipeline for {pod_id} (max batch size: {max_batch_size})")

    config = load_config('config.json')
    db_config = config['db_config']

    # Generate unique IDs once for the entire pipeline run
    conn, cur = initialize_db_connection(db_config)
    iterationid = get_or_create_iterationid(conn)

    cur.execute("SELECT MAX(stagingid) FROM orgi.visibilityitemsstaging")
    result = cur.fetchone()
    stagingid = (result[0] if result[0] is not None else 0) + 1
    logger.info(f"Using stagingid: {stagingid} for this entire run")

    # ── Initial state for bot ──────────────────────────────────────────────────
    _update_state(
        pod_id=pod_id,
        iterationid=iterationid,
        stagingid=stagingid,
        current_stage="Clearing Temp Tables",
    )

    # Clear temp tables once before batch loop
    logger.info("=" * 60)
    logger.info(f"CLEARING TEMP TABLES (iteration {iterationid})")
    logger.info("=" * 60)
    try:
        cur.execute("DELETE FROM temp.sku_prediction_temp WHERE iteration_id = %s", (iterationid,))
        deleted_sku = cur.rowcount
        cur.execute("DELETE FROM temp.cap_prediction_temp WHERE iteration_id = %s", (iterationid,))
        deleted_cap = cur.rowcount
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'temp' AND table_name = 'structural_count_temp'
        """)
        if cur.fetchone() is not None:
            cur.execute(
                "DELETE FROM temp.structural_count_temp WHERE iteration_id = %s",
                (iterationid,)
            )
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'temp' AND table_name = 'sku_planogram_temp'
        """)
        deleted_planogram = 0
        if cur.fetchone() is not None:
            cur.execute(
                "DELETE FROM temp.sku_planogram_temp WHERE iteration_id = %s",
                (iterationid,)
            )
            deleted_planogram = cur.rowcount
        conn.commit()
        logger.info(
            f"Cleared {deleted_sku} SKU records, {deleted_cap} cap records, "
            f"{deleted_planogram} planogram SKU records"
        )
        logger.info("Temp tables ready — batches will APPEND data")
    except Exception as e:
        logger.error(f"Failed to clear temp tables: {e}")
        conn.rollback()

    # Get initial unprocessed store count for the start notification
    unprocessed_stores_initial = get_unprocessed_store_count(conn)
    close_db_connection(conn, cur)

    # ── Start Telegram bot threads ─────────────────────────────────────────────
    heartbeat = PipelineHeartbeat(get_status_fn=_get_state)
    qa_bot    = PipelineQABot(get_status_fn=_get_state)
    heartbeat.start()
    qa_bot.start()

    # ── Notify pipeline start ──────────────────────────────────────────────────
    notify_pipeline_start(pod_id, iterationid, stagingid, unprocessed_stores_initial)

    logger.info("=" * 60)
    logger.info("Pipeline Run Configuration:")
    logger.info(f"  Iteration ID : {iterationid} (shared across all batches)")
    logger.info(f"  Staging ID   : {stagingid} (shared across all batches)")
    logger.info(f"  Vision model : YOLO activation only (no LLM)")
    logger.info("=" * 60)

    batch_number = 0
    cap_status   = "not_run"

    try:
        while True:
            try:
                conn, cur = initialize_db_connection(db_config)

                reset_stale_batches(conn, STALE_TIMEOUT_MINUTES)

                unprocessed_stores = get_unprocessed_store_count(conn)
                _update_state(remaining_stores=unprocessed_stores)

                if unprocessed_stores == 0:
                    logger.info("=" * 60)
                    logger.info("All stores processed successfully")
                    logger.info(f"  Iteration ID  : {iterationid}")
                    logger.info(f"  Staging ID    : {stagingid}")
                    logger.info(f"  Total batches : {batch_number}")
                    logger.info("=" * 60)
                    close_db_connection(conn, cur)

                    # ── CAP Post-Processing Pipeline ──────────────────────────
                    _update_state(current_stage="CAP Post-Processing")
                    logger.info("=" * 60)
                    logger.info("Starting CAP prediction post-processing pipeline...")
                    logger.info("=" * 60)
                    try:
                        cap_result = run_cap_pipeline(
                            db_config=db_config,
                            iteration_id=iterationid,
                            staging_id=stagingid,
                        )
                        if cap_result.overall_status != "success":
                            cap_status = cap_result.overall_status
                            logger.error("CAP pipeline finished with errors")
                            notify_cap_pipeline(cap_status, cap_result.total_duration_ms)
                        else:
                            cap_status = "success"
                            logger.info(
                                f"CAP pipeline completed successfully "
                                f"in {cap_result.total_duration_ms:.0f} ms."
                            )
                            notify_cap_pipeline("success", cap_result.total_duration_ms)
                    except Exception as e:
                        cap_status = "error"
                        msg = f"CAP pipeline exception: {e}"
                        logger.error(msg)
                        logger.error(traceback.format_exc())
                        _record_error(msg)
                        notify_pipeline_error("CAP Pipeline", str(e), pod_id)

                    # ── SKU-vs-CAP Reconciliation ──────────────────────────────
                    # Must run AFTER run_cap_pipeline (needs the master/transaction
                    # rows it just wrote) and BEFORE run_planogram_pipeline (its
                    # tranid-offset logic reads MAX(iterationtranid) from
                    # orgi.coolermetricsmaster, so any tranid this step mints for
                    # an orphan image has to land before that read happens).
                    # Only run it if the CAP pipeline actually succeeded — its
                    # reconciliation depends on temp.cap_prediction_temp and
                    # orgi.coolermetricsmaster already being fully populated for
                    # this iteration.
                    if cap_status == "success":
                        _update_state(current_stage="SKU vs CAP Reconciliation")
                        logger.info("=" * 60)
                        logger.info(f"Running SKU-vs-CAP reconciliation for iteration {iterationid}...")
                        logger.info("=" * 60)
                        try:
                            reconciliation_result = run_reconciliation(
                                db_config=db_config,
                                iteration_id=iterationid,
                                dry_run=False,
                            )
                            logger.info(
                                f"Reconciliation done: "
                                f"{len(reconciliation_result['reconciled'])} shelf(es) fixed, "
                                f"{reconciliation_result['skipped']} skipped, "
                                f"{len(reconciliation_result['orphans_reconciled'])} orphan(s) fixed, "
                                f"{len(reconciliation_result['ambiguous'])} ambiguous "
                                f"(iteration {iterationid})"
                            )
                            if reconciliation_result['ambiguous']:
                                logger.warning(
                                    f"{len(reconciliation_result['ambiguous'])} image(s) had "
                                    f"multiple iterationtranid values and were skipped for "
                                    f"manual review: {reconciliation_result['ambiguous']}"
                                )
                        except Exception as e:
                            msg = f"SKU-vs-CAP reconciliation exception: {e}"
                            logger.error(msg)
                            logger.error(traceback.format_exc())
                            _record_error(msg)
                            notify_pipeline_error("SKU vs CAP Reconciliation", str(e), pod_id)
                    else:
                        logger.warning(
                            "Skipping SKU-vs-CAP reconciliation because the CAP "
                            f"pipeline did not complete successfully (status={cap_status})"
                        )

                    # ── Planogram Pipeline (subcategory 603) ──────────────────
                    # Runs AFTER run_cap_pipeline for this same iterationid:
                    # its iterationtranid-offset logic reads
                    # orgi.coolermetricsmaster's current max tranid to keep
                    # planogram tranids from colliding with 605's in the
                    # shared orgi.shelfsequencecompliance table downstream.
                    _update_state(current_stage="Planogram Pipeline")
                    logger.info("=" * 60)
                    logger.info("Starting planogram post-processing pipeline...")
                    logger.info("=" * 60)
                    try:
                        planogram_result = run_planogram_pipeline(
                            db_config=db_config,
                            iteration_id=iterationid,
                        )
                        if planogram_result.overall_status != "success":
                            logger.error("Planogram pipeline finished with errors")
                            notify_pipeline_error(
                                "Planogram Pipeline", "One or more steps failed", pod_id
                            )
                        else:
                            logger.info(
                                f"Planogram pipeline completed successfully "
                                f"in {planogram_result.total_duration_ms:.0f} ms."
                            )
                    except Exception as e:
                        msg = f"Planogram pipeline exception: {e}"
                        logger.error(msg)
                        logger.error(traceback.format_exc())
                        _record_error(msg)
                        notify_pipeline_error("Planogram Pipeline", str(e), pod_id)

                    # ── Shelf-Sequence Compliance Check ───────────────────────
                    _update_state(current_stage="Shelf Sequence Check")
                    logger.info("=" * 60)
                    logger.info(f"Running shelf-sequence check for iteration {iterationid}...")
                    logger.info("=" * 60)
                    try:
                        shelf_results = run_shelf_sequence_check(
                            db_config=db_config,
                            iterationid=iterationid,
                        )
                        shelf_passed = sum(1 for r in shelf_results if r[3] == 'Y')
                        logger.info(
                            f"Shelf-sequence check done: {shelf_passed}/{len(shelf_results)} "
                            f"images compliant (iteration {iterationid})"
                        )

                        # Reclassify low-detection-count IMPURE rows back to
                        # PURE. Must run AFTER run_shelf_sequence_check(),
                        # since that's what populates purity_status and
                        # dino_nonbeverage_detections for this iteration in
                        # the first place — running it any earlier (e.g. as
                        # a step inside run_cap_pipeline, which executes
                        # before this check) always matched 0 rows because
                        # the data it depends on didn't exist yet.
                        reclassified = reclassify_low_detection_impure(
                            db_config=db_config,
                            iteration_id=iterationid,
                        )
                        logger.info(
                            f"Reclassified {reclassified} low-detection-count "
                            f"IMPURE row(s) back to PURE (iteration {iterationid})"
                        )
                    except Exception as e:
                        msg = f"Shelf-sequence check exception: {e}"
                        logger.error(msg)
                        logger.error(traceback.format_exc())
                        _record_error(msg)
                        notify_pipeline_error("Shelf Sequence Check", str(e), pod_id)

                    # ── Planogram Shelf-Sequence Compliance Check ─────────────
                    _update_state(current_stage="Planogram Shelf Sequence Check")
                    logger.info("=" * 60)
                    logger.info(f"Running planogram shelf-sequence check for iteration {iterationid}...")
                    logger.info("=" * 60)
                    try:
                        planogram_shelf_results = run_shelf_sequence_check_planogram(
                            db_config=db_config,
                            iterationid=iterationid,
                        )
                        planogram_shelf_passed = sum(
                            1 for r in planogram_shelf_results if r[3] == 'Y'
                        )
                        logger.info(
                            f"Planogram shelf-sequence check done: "
                            f"{planogram_shelf_passed}/{len(planogram_shelf_results)} "
                            f"images compliant (iteration {iterationid})"
                        )
                    except Exception as e:
                        msg = f"Planogram shelf-sequence check exception: {e}"
                        logger.error(msg)
                        logger.error(traceback.format_exc())
                        _record_error(msg)
                        notify_pipeline_error("Planogram Shelf Sequence Check", str(e), pod_id)
                    break

                logger.info("=" * 60)
                logger.info(f"Unprocessed stores remaining: {unprocessed_stores}")
                logger.info("=" * 60)

                # Recompute every iteration: if fewer than max_batch_size stores
                # are waiting, process all of them (batch = unprocessed_stores);
                # once 50+ are waiting, cap the batch at max_batch_size and let
                # the remainder (e.g. 1 leftover store) form its own batch next
                # time round. This is why we don't ask/cache a batch_size once.
                current_batch_size = min(max_batch_size, unprocessed_stores)
                logger.info(
                    f"Batch size for this round: {current_batch_size} "
                    f"(max {max_batch_size}, {unprocessed_stores} unprocessed)"
                )

                assigned_stores, assigned_files = assign_stores_to_pod(conn, current_batch_size, pod_id)
                close_db_connection(conn, cur)

                if assigned_stores == 0:
                    logger.warning("No stores assigned. Retrying in 10 seconds")
                    time.sleep(10)
                    continue

                batch_number += 1
                _update_state(batch_number=batch_number, current_stage="Processing Batch")
                logger.info(
                    f"Starting batch {batch_number}: "
                    f"{assigned_stores} stores, {assigned_files} images"
                )
                notify_batch_start(batch_number, assigned_stores, assigned_files, pod_id)

                result = execute_models(pod_id, iterationid, stagingid)

                # execute_models now returns (success, stores, images, visicooler, yolo)
                if isinstance(result, tuple):
                    success, b_stores, b_images, b_visicooler, b_yolo = result
                else:
                    success = result
                    b_stores = b_images = b_visicooler = b_yolo = 0

                if not success:
                    logger.warning(f"Batch {batch_number} failed. Waiting 10 seconds before retry")
                    notify_batch_failed(batch_number, "execute_models returned False", pod_id)
                    time.sleep(10)
                else:
                    # Refresh remaining count for the notification
                    conn2, cur2 = initialize_db_connection(db_config)
                    remaining_after = get_unprocessed_store_count(conn2)
                    close_db_connection(conn2, cur2)
                    _update_state(remaining_stores=remaining_after)

                    notify_batch_complete(
                        batch_number, b_stores, b_images,
                        b_visicooler, b_yolo, remaining_after
                    )
                    logger.info(f"Batch {batch_number} completed. Checking for next batch")
                    time.sleep(5)

            except KeyboardInterrupt:
                raise
            except Exception as e:
                msg = f"Main loop error: {e}"
                logger.error(msg)
                logger.error(traceback.format_exc())
                _record_error(msg)
                notify_pipeline_error("Main Loop", str(e), pod_id)
                time.sleep(10)

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        notify_pipeline_error("User Interrupt", "Pipeline stopped by operator (KeyboardInterrupt)", pod_id)

    finally:
        # ── Stop bot threads ───────────────────────────────────────────────────
        heartbeat.stop()
        qa_bot.stop()

        # ── Final notification ─────────────────────────────────────────────────
        notify_pipeline_complete(pod_id, iterationid, stagingid, batch_number, cap_status)

    logger.info(f"Pipeline execution completed for {pod_id}")


if __name__ == "__main__":
    main()