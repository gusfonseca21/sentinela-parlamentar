from datetime import date

import pytest

from src.utils.camara import get_legislatura_data


@pytest.fixture
def valid_legislatura_data():
    """
    Exemplo de um objeto de Legislatura da Câmara.
    """
    return {
        "dados": [
            {
                "id": 57,
                "uri": "https://dadosabertos.camara.leg.br/api/v2/legislaturas/57",
                "dataInicio": "2023-02-01",
                "dataFim": "2027-01-31",
            }
        ],
        "links": [
            {
                "rel": "self",
                "href": "https://dadosabertos.camara.leg.br/api/v2/legislaturas?data=2023-02-01",
            },
            {
                "rel": "first",
                "href": "https://dadosabertos.camara.leg.br/api/v2/legislaturas?data=2023-02-01&pagina=1&itens=15",
            },
            {
                "rel": "last",
                "href": "https://dadosabertos.camara.leg.br/api/v2/legislaturas?data=2023-02-01&pagina=1&itens=15",
            },
        ],
    }


@pytest.fixture
def empty_dados_legislatura():
    """Legislatura sem dados."""
    return {"dados": [], "links": []}


@pytest.fixture
def missing_dados_key():
    """Objeto sem a chave 'dados'."""
    return {"links": []}


@pytest.fixture
def invalid_id_legislatura():
    """Legislatura com id inválido."""
    return {
        "dados": [
            {"id": "não-é-número", "dataInicio": "2023-02-01", "dataFim": "2027-01-31"}
        ]
    }


@pytest.fixture
def invalid_date_format():
    """Legislatura com formato de data inválido."""
    return {
        "dados": [
            {
                "id": 57,
                "dataInicio": "01/02/2023",  # formato errado
                "dataFim": "2027-01-31",
            }
        ]
    }


@pytest.fixture
def missing_property():
    """Legislatura sem uma propriedade esperada."""
    return {
        "dados": [
            {
                "dataInicio": "2023-02-01",
                "dataFim": "2027-01-31",
                # falta 'id'
            }
        ]
    }


# ============= HAPPY PATH TESTS =============


def test_get_id_success(valid_legislatura_data):
    """Testa a extração e conversão de Id de Legislatura para número inteiro."""
    result = get_legislatura_data(valid_legislatura_data, "id")
    assert isinstance(result, int)


def test_get_start_date_success(valid_legislatura_data):
    """Teste para extração e conversão de Data de Início de uma Legislatura para um dicionário de data."""
    result = get_legislatura_data(valid_legislatura_data, "dataInicio")
    assert isinstance(result, date)


def test_gest_end_date_success(valid_legislatura_data):
    """Teste para extração e conversão de Data de Fim de uma Legislatura para um dicionário de data."""
    result = get_legislatura_data(valid_legislatura_data, "dataFim")
    assert isinstance(result, date)


# ============= ERROR CASE TESTS =============


def test_empty_dados_raises_error(empty_dados_legislatura):
    """Testa se levanta erro quando 'dados' está vazio."""
    with pytest.raises(
        ValueError, match="Não foram encontrados dados sobre Legislatura"
    ):
        get_legislatura_data(empty_dados_legislatura, "id")


def test_missing_dados_key_raises_error(missing_dados_key):
    """Testa se levanta erro quando a chave 'dados' não existe."""
    with pytest.raises(
        ValueError, match="Não foram encontrados dados sobre Legislatura"
    ):
        get_legislatura_data(missing_dados_key, "id")


def test_missing_property_raises_error(missing_property):
    """Testa se levanta erro quando uma propriedade não existe."""
    with pytest.raises(
        ValueError, match="A propriedade 'id' não existe dentro de Legislatura"
    ):
        get_legislatura_data(missing_property, "id")


def test_invalid_id_conversion_raises_error(invalid_id_legislatura):
    """Testa se levanta erro quando o id não é conversível para inteiro."""
    with pytest.raises(ValueError, match="não é conversível para um número inteiro"):
        get_legislatura_data(invalid_id_legislatura, "id")


def test_invalid_date_format_raises_error(invalid_date_format):
    """Testa se levanta erro quando a data está em formato inválido."""
    with pytest.raises(ValueError, match="não é conversível para um objeto de data"):
        get_legislatura_data(invalid_date_format, "dataInicio")


# ============= EDGE CASE TESTS =============


def test_id_zero_is_valid():
    """Testa se id = 0 é considerado válido."""
    data = {"dados": [{"id": 0, "dataInicio": "2023-02-01", "dataFim": "2027-01-31"}]}
    result = get_legislatura_data(data, "id")
    assert result == 0


def test_id_as_string_number_converts():
    """Testa que id como string numérica é convertida corretamente."""
    data = {
        "dados": [{"id": "57", "dataInicio": "2023-02-01", "dataFim": "2027-01-31"}]
    }
    result = get_legislatura_data(data, "id")
    assert result == 57
    assert isinstance(result, int)
