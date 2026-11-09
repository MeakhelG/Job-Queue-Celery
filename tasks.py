from celery_config import app
from celery.result import allow_join_result
import time


# Task 1 (ringan): Memproses input data
@app.task(bind=True, queue='data_processing')
def process_data(self, raw_data):
    """
    Task 1: Memproses input data
    Queue: 'data_processing'
    """
    try:
        print(f"[process_data] Processing data: {raw_data}")

        # Memproses... (di sini dimisalkan dengan sleep())
        time.sleep(2)

        processed_data = {
            'original': raw_data,
            'processed_at': time.time(),
            'word_count': len(str(raw_data).split()) if isinstance(raw_data, str) else 0,
            'status': 'processed',
        }

        print(f"[process_data] Completed: {processed_data}")
        return processed_data

    except Exception as exc:
        print(f"[process_data] Error: {exc}")
        self.retry(countdown=60, max_retries=3, exc=exc)


# Task 2 (berat): Membuat report (laporan)
@app.task(bind=True, queue='report_generation')
def generate_report(self, processed_data):
    """
    Task 2: Membuat report dari data yang terproses
    Queue: 'report_generation'
    """
    try:
        print(f"[generate_report] Generating report from: {processed_data}")

        time.sleep(3)

        report = {
            'report_id': f"RPT_{int(time.time())}",
            'data_summary': {
                'original_data': processed_data.get('original'),
                'word_count': processed_data.get('word_count', 0),
                'processed_at': processed_data.get('processed_at'),
            },
            'generated_at': time.time(),
            'status': 'completed',
        }

        print(f"[generate_report] Completed: {report}")
        return report

    except Exception as exc:
        print(f"[generate_report] Error: {exc}")
        self.retry(countdown=60, max_retries=3, exc=exc)


# Orchestrator (chain manual Task 1 -> Task 2)
@app.task(bind=True)
def process_and_report(self, raw_data):
    """
    Task orchestrator: merangkai (chain) Task 1 dan 2.
    """
    try:
        # allow_join_result() diperlukan karena Celery secara default melarang
        # task memanggil .get() di dalam task lain (sinkronisasi dalam worker
        # beresiko deadlock). Kita sadari resikonya dan izinkan di sini.
        with allow_join_result():
            process_result = process_data.delay(raw_data)
            processed_data = process_result.get()

            report_result = generate_report.delay(processed_data)
            final_report = report_result.get()

        return final_report

    except Exception as exc:
        print(f"[process_and_report] Error: {exc}")
        raise
