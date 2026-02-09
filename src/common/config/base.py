from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import Optional

class BaseConfig(BaseSettings):
    """Base configuration with validation"""
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "validate_assignment": True,
        "extra": "ignore",  # Allow extra env vars in .env file
    }
        
    @field_validator("*", mode="before")
    @classmethod
    def validate_required(cls, v, info):
        if v is None and info.field_name:
            # Check if field is required (has no default)
            field_info = cls.model_fields.get(info.field_name)
            if field_info and field_info.is_required():
                raise ValueError(f"{info.field_name} is required")
        return v