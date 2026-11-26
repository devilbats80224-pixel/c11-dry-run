"""Lexical screen, not eligibility. Never transform an exchange contract ID."""
import re

# Preserve the existing ASCII vocabulary and permit Han ideographs in original form.
ID_CHAR=r'[A-Z0-9\u3400-\u4DBF\u4E00-\u9FFF\U00020000-\U0002A6DF\U0002A700-\U0002EBEF\U00030000-\U000323AF]'

def identity_shape(value):
    return type(value) is str and re.fullmatch(ID_CHAR+r'{2,40}',value) is not None

def quote_contract_shape(value):
    return type(value) is str and value.endswith('USDT') and identity_shape(value)
