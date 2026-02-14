import pytest
import requests

from src.utils.io import fetch_html_many_async


@pytest.fixture
def urls_paginas_html_deputados() -> list[str]:
    """
    Série de URLs das páginas HTML de deputados.
    """

    deputados_url = "https://dadosabertos.camara.leg.br/api/v2/deputados?idLegislatura=57&ordem=ASC&ordenarPor=nome"

    response = requests.get(deputados_url)
    response.raise_for_status()
    data = response.json()

    ids_deputados = {deputado.get("id", 0) for deputado in data.get("dados", [])}

    paginas_deputados_urls = [
        f"https://www.camara.leg.br/deputados/{id}" for id in ids_deputados
    ]

    return paginas_deputados_urls


# ============= TESTS =============


@pytest.mark.asyncio
async def test_fetch_html_many_async(urls_paginas_html_deputados):
    """
    Teste de caso de sucesso da função fetch_html_many_async.
    """

    urls = urls_paginas_html_deputados
    expected_count = len(urls_paginas_html_deputados)

    results = await fetch_html_many_async(urls=urls, lote_id=9999, task="teste")

    items_downloaded = len(results)

    assert len(results) > 0, "Nenhum resultado foi retornado!"
    assert items_downloaded == expected_count, (
        f"Esperava por {expected_count} resultados baixados, mas retornaram {items_downloaded}"
    )
