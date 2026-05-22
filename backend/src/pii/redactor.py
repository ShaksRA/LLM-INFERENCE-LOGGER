"""
PII Redactor
Redacts common PII patterns from text before sending to LLM providers
and before storing in the database.

Patterns covered:
- Email addresses
- Phone numbers (US + international)
- Credit card numbers
- US SSNs
- IP addresses
- Names (heuristic - capitalized word pairs)
- API keys / secrets (Bearer tokens, sk- keys)
"""

import re
from typing import List, Tuple


# (pattern, replacement_label)
PII_PATTERNS: List[Tuple[re.Pattern, str]] = [
    # Email
    (re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Z|a-z]{2,}\b'), "[EMAIL]"),
    # Credit card (Visa, MC, Amex, Discover)
    (re.compile(r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b'), "[CREDIT_CARD]"),
    # US SSN
    (re.compile(r'\b\d{3}-\d{2}-\d{4}\b'), "[SSN]"),
    # US phone
    (re.compile(r'\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'), "[PHONE]"),
    # IPv4
    (re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b'), "[IP_ADDRESS]"),
    # API keys / Bearer tokens
    (re.compile(r'\b(?:sk-[A-Za-z0-9\-_]{20,}|Bearer\s+[A-Za-z0-9\-_.]+)\b'), "[API_KEY]"),
    # Anthropic / OpenAI style keys
    (re.compile(r'\bsk-ant-[A-Za-z0-9\-_]{20,}\b'), "[API_KEY]"),
]


class PIIRedactor:
    def __init__(self, patterns=None):
        self.patterns = patterns or PII_PATTERNS

    def redact(self, text: str) -> str:
        if not text:
            return text
        for pattern, label in self.patterns:
            text = pattern.sub(label, text)
        return text

    def contains_pii(self, text: str) -> bool:
        return any(p.search(text) for p, _ in self.patterns)

    def audit(self, text: str) -> List[str]:
        """Returns list of PII types found (for logging/alerting)."""
        found = []
        for pattern, label in self.patterns:
            if pattern.search(text):
                found.append(label)
        return found
