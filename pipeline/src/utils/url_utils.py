from datetime import date
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


def is_first_page(url: str) -> bool:
    """ADICIONAR ESSA FUNÇÃO AQUI NO UTITLS DE URL"""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    # Se não tem 'pagina' OU é '1', é primeira página
    pagina = params.get("pagina", ["1"])[0]
    return str(pagina) in ["1", ""]


def generate_date_urls_senado(
    url: str, start_date: date, end_date: date
) -> list[str] | None:
    """
    Calcula e retorna as URLs necessárias para baixar os dados do Senado de acordo com as datas de início e fim selecionadas. A maioria dos endpoints do Senado aceitam apenas a diferença de um ano entre esses argumentos.
    """
    if "%STARTDATE%" not in url or "%ENDDATE%" not in url:
        raise ValueError(
            f"A URL não possui as strings corretas de '%STARTDATE%' ou '%ENDDATE' para realizara a substituição: {url}"
        )

    if end_date < start_date:
        raise ValueError(
            f"A data de início ({start_date}) não pode ser maior que a data de fim ({end_date})"
        )

    if start_date.year == end_date.year:
        url_ret = url.replace("%STARTDATE%", start_date.isoformat()).replace(
            "%ENDDATE%", end_date.isoformat()
        )
        return [url_ret]

    if start_date.year != end_date.year:
        urls_ret = []
        for year in range(start_date.year, (end_date.year + 1)):
            if year == start_date.year:
                url_ret = url.replace("%STARTDATE%", start_date.isoformat()).replace(
                    "%ENDDATE%", f"{year}-12-31"
                )
                urls_ret.append(url_ret)

            if year != start_date.year and year != end_date.year:
                url_ret = url.replace("%STARTDATE%", f"{year}-01-01").replace(
                    "%ENDDATE%", f"{year}-12-31"
                )
                urls_ret.append(url_ret)

            if year == end_date.year:
                url_ret = url.replace("%STARTDATE%", f"{year}-01-01").replace(
                    "%ENDDATE%", end_date.isoformat()
                )
                urls_ret.append(url_ret)

        return urls_ret
