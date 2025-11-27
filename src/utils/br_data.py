from datetime import datetime

BR_STATES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

# Senadores cumprem duas legislaturas (8 anos) em seus mandatos
def calculate_election_years(current_date: int = datetime.now().year) -> list[int]:
    base_election_year = 2018

    years_since_base = current_date - base_election_year
    most_recent_election = base_election_year + (years_since_base // 4) * 4
    oldest_election = most_recent_election - 4

    return [oldest_election, most_recent_election]