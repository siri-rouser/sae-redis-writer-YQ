import logging
import signal
import threading

from prometheus_client import Counter, Histogram, start_http_server
from visionlib.pipeline.consumer import RedisConsumer

from .config import RedisWriterConfig
from .saedumper import Saedumper
from .sender import Sender

logger = logging.getLogger(__name__)

PROMETHEUS_METRICS_PORT = 8000

FRAME_COUNTER = Counter('redis_writer_frame_counter', 'How many frames have been consumed from the Redis input stream')

def run_stage():

    stop_event = threading.Event()

    # Register signal handlers
    def sig_handler(signum, _):
        signame = signal.Signals(signum).name
        print(f'Caught signal {signame} ({signum}). Exiting...')
        stop_event.set()

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    CONFIG = RedisWriterConfig()

    logger.setLevel(CONFIG.log_level.value)

    logger.info(f'Starting prometheus metrics endpoint on port {PROMETHEUS_METRICS_PORT}')

    start_http_server(PROMETHEUS_METRICS_PORT)

    logger.info(f'Starting redis writer stage. Config: {CONFIG.model_dump_json(indent=2)}')

    sae_writer = Saedumper(CONFIG)

    sender = Sender(CONFIG)
    
    with sender as send:
        while True:
            if stop_event.is_set():
                break

            FRAME_COUNTER.inc()

            output_proto_data, stream_id = sae_writer.get()

            if output_proto_data is None:
                continue

            if stream_id and isinstance(stream_id, str):
                send(f'{CONFIG.target_redis.output_stream_prefix}:{stream_id.split(":")[-1]}', output_proto_data)
            else:
                logger.warning(f'Invalid stream_id: {stream_id}')