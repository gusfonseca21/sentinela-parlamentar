from datetime import date, datetime
from typing import Literal

LegislaturaProps = Literal["id", "dataInicio", "dataFim"]


def get_legislatura_data(
    legislatura_dict: dict, property: LegislaturaProps
) -> int | date:
    """
    Extrai e converte dados de uma propriedade específica relacionadas ao objeto Legislatura (CÂMARA).
    """
    # Verificando se existe a chave 'dados' dentro do objeto Legislatura
    leg_data = legislatura_dict.get("dados", [])
    if len(leg_data) < 1:
        raise ValueError(
            f"Não foram encontrados dados sobre Legislatura no objeto passado: {leg_data}"
        )

    prop_data = leg_data[0].get(property, None)
    if prop_data is None:
        raise ValueError(
            f"A propriedade '{property}' não existe dentro de Legislatura. Propriedades disponíveis: {prop_data}"
        )

    if property == "id":
        try:
            return int(prop_data)
        except ValueError:
            raise ValueError(
                f"O valor de '{property}' ('{prop_data}') não é conversível para um número inteiro."
            )
    else:
        date_format = "%Y-%m-%d"
        try:
            return datetime.strptime(str(prop_data), date_format).date()
        except ValueError:
            raise ValueError(
                f"O valor de '{property}' ('{prop_data}') não é conversível para um objeto de data."
            )
