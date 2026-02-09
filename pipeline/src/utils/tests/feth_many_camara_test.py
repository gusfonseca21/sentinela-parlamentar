import pytest
import requests

from ..fetch_many_jsons import fetch_many_camara


@pytest.fixture
def valid_non_paginated() -> list[str]:
    """
    Série de URLs com resultados que não são paginados.
    """

    deputados_url = "https://dadosabertos.camara.leg.br/api/v2/deputados?idLegislatura=57&ordem=ASC&ordenarPor=nome"

    response = requests.get(deputados_url)
    response.raise_for_status()
    data = response.json()

    ids_deputados = {deputado.get("id", 0) for deputado in data.get("dados", [])}

    detalhes_deputados_urls = [
        f"https://dadosabertos.camara.leg.br/api/v2/deputados/{id}"
        for id in ids_deputados
    ]

    return detalhes_deputados_urls


# ============= TESTS =============


@pytest.mark.asyncio
async def test_fetch_many_camara_non_paginated(valid_non_paginated):
    """
    Teste da função fetch_many_camara para lista de URLs não paginadas.
    """
    urls = valid_non_paginated
    expected_count = len(valid_non_paginated)

    results = await fetch_many_camara(urls=urls, follow_pagination=False)

    items_downloaded = len(results)

    assert len(results) > 0, "Nenhum resultado foi retornado!"
    assert items_downloaded == expected_count, (
        f"Esperava por {expected_count} resultados baixados, mas retornaram {items_downloaded}"
    )


@pytest.mark.asyncio
async def test_fetch_many_camara_paginated():
    """
    Teste da função test_fetch_many_camara_paginated para lista de URLs paginadas.
    """

    # Discursos de deputados entre 2024-01-01 e 2024-12-31

    # Glauber Braga: 85 discursos
    url_1 = "https://dadosabertos.camara.leg.br/api/v2/deputados/152605/discursos?dataInicio=2024-01-01&dataFim=2024-12-31&ordenarPor=dataHoraInicio&ordem=DESC&itens=10"

    # Marcel Van Hattem: 248 discrusos
    url_2 = "https://dadosabertos.camara.leg.br/api/v2/deputados/156190/discursos?dataInicio=2024-01-01&dataFim=2024-12-31&ordenarPor=dataHoraInicio&ordem=DESC&itens=10"

    # Sâmia Bonfim: 40 discursos
    url_3 = "https://dadosabertos.camara.leg.br/api/v2/deputados/204535/discursos?dataInicio=2024-01-01&dataFim=2024-12-31&ordenarPor=dataHoraInicio&ordem=DESC&itens=10"

    expected_count = 373

    results = await fetch_many_camara(
        urls=[url_1, url_2, url_3], follow_pagination=True, limit=30
    )

    items_downloaded = 0
    for r in results:
        num_r_items = len(r.get("dados", []))  # type: ignore
        items_downloaded += num_r_items

    assert len(results) > 0, "Nenhum resultado foi retornado!"
    assert items_downloaded == expected_count, (
        f"Esperava por {expected_count} items baixados, mas recebeu {items_downloaded}"
    )
