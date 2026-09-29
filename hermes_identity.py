"""Hermes-only provider/model identity. No registry lookup or global defaults."""
from dataclasses import dataclass
import re

@dataclass(frozen=True)
class ProviderModelIdentity:
    provider: str
    model: str

    @classmethod
    def parse(cls, value, *, provider=''):
        value = str(value)
        if '/' in value:
            actual_provider, model = value.split('/', 1)
            if provider and provider != actual_provider:
                raise ValueError('HERMES_PROVIDER_MISMATCH')
        else:
            actual_provider, model = provider, value
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', actual_provider) or not re.fullmatch(r'[A-Za-z0-9_./:-]+', model):
            raise ValueError('HERMES_PROVIDER_MODEL_REQUIRED')
        return cls(actual_provider, model)

    @property
    def qualified(self):
        return self.provider + '/' + self.model
