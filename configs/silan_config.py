import sys
sys.path.insert(0,r'./')
from .config import Config
from dataclasses import dataclass, asdict, fields
from typing import Dict, List

@dataclass
class SilanConfig(Config):
    entity_type: str
    entity_id: str
    source_translation_id: str
    source_lang: str
    target_lang: str
    title: str
    synopsis: str
    content: str

    @property
    def __repr__(self) -> str:
        preview_bits = [
            f"Example: {self.qas_id}",
            f"Entity: {self.entity_type} ({self.entity_id})",
            f"Lang: {self.source_lang} -> {self.target_lang}",
        ]
        if self.title:
            preview_bits.append(f"Title: {self.title[:60]}")
        if self.synopsis:
            preview_bits.append(f"Synopsis: {self.synopsis[:60]}")
        if self.content:
            preview_bits.append(f"Content: {self.content[:60]}")
        return " | ".join(preview_bits)

    @property
    def get_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def get_keys(cls) -> List[str]:
        return [field.name for field in fields(cls)]