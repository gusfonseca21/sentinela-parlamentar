from datetime import datetime

BR_UFS = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]


# Senadores cumprem duas legislaturas (8 anos) em seus mandatos
def get_election_years(current_year: int) -> list[int]:
    oldest_election = 2018
    most_recent_election = 2022

    if current_year > 2022 and current_year < 2027:
        oldest_election = 2018
        most_recent_election = 2022

    if current_year > 2026 and current_year < 2030:
        oldest_election = 2022
        most_recent_election = 2026

    if current_year > 2030 and current_year < 2034:
        oldest_election = 2026
        most_recent_election = 2030

    return [oldest_election, most_recent_election]
