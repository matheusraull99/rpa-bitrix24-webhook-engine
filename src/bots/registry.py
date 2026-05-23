"""Registry de bots: action -> função executora."""
from importlib import import_module
from typing import Callable

BOTS: dict[str, str] = {
    "consulta_inss": "src.bots.demo_bot:executar",
    "baixa_comprovante": "src.bots.demo_bot:executar",
}


def resolve(action: str) -> Callable:
    if action not in BOTS:
        raise KeyError(f"Bot desconhecido: {action}")
    mod_path, fn_name = BOTS[action].split(":")
    mod = import_module(mod_path)
    return getattr(mod, fn_name)
