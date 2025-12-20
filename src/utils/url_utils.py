from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


def get_query_param_value(url: str, param_name: str, default_value: str | int) -> Any:
    """
    Busca o valor de um parâmetro específico de uma URL. Se o parâmetro não estiver presente, retorna o valor padrão.
    """
    parsed_url = urlparse(url)
    params = parse_qs(parsed_url.query)
    param_value = params.get(param_name, [default_value])[0]
    return param_value


def alter_query_param_value(base_url: str, param_name: str, new_value) -> str:
    """
    Substitui o valor de um parâmetro específico de uma URL, retornando uma nova URL com o novo valor.
    Se o parâmetro não existir, ele é criado.
    """
    parsed_base_url = urlparse(base_url)
    base_params = parse_qs(parsed_base_url.query)
    base_params[param_name] = [str(new_value)]

    new_query = urlencode(base_params, doseq=True)
    new_url = urlunparse(parsed_base_url._replace(query=new_query))
    return new_url


def get_path_parameter_value(url: str, param_name: str) -> Any:
    path_parts = urlparse(url).path.strip("/").split("/")

    param_index = path_parts.index(param_name)

    return path_parts[param_index + 1]
