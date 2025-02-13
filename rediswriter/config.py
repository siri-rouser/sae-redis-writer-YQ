from typing import List

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Annotated
from visionlib.pipeline.settings import LogLevel, YamlConfigSettingsSource


class RedisConfig(BaseModel):
    host: str = 'localhost'
    port: Annotated[int, Field(ge=1, le=65536)] = 6379
    input_stream_prefix: str = 'geomapper'

class TargetRedisConfig(BaseModel):
    host: str
    port: Annotated[int, Field(ge=1, le=65536)]
    output_stream_prefix: str = 'output'
    buffer_length: Annotated[int, Field(ge=1)] = 100
    target_stream_maxlen: Annotated[int, Field(ge=1)] = 100
    tls: bool = False

class RedisWriterConfig(BaseSettings):
    log_level: LogLevel = LogLevel.WARNING
    redis: RedisConfig = RedisConfig()
    target_redis: TargetRedisConfig
    stream_ids: List[str] = ['stream1']
    remove_frame_data: bool = True

    model_config = SettingsConfigDict(env_nested_delimiter='__')

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings):
        return (init_settings, env_settings, YamlConfigSettingsSource(settings_cls), file_secret_settings)