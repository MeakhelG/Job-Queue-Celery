from celery import chain
from tasks import process_data, generate_report


def create_task_chain(raw_data):
    """
    Menggunakan chain primitive milik Celery.
    Output dari process_data otomatis menjadi input generate_report.
    """
    job = chain(
        process_data.s(raw_data),
        generate_report.s(),
    )
    return job.apply_async()
