from datetime import date, timedelta

def urls_despesas(deputados_ids: list[int], start_date: date, legislatura: dict) -> list[str]:
    id_legislatura = legislatura.get("dados", [])[0].get("id")
    today = date.today()
    # Se start_date for menor que o ano atual, irá baixar todos os dados de despesas
    if start_date.year <  today.year:
        return [f"deputados/{id}/despesas?idLegislatura={id_legislatura}&itens=1000" for id in deputados_ids]
    else:
        # O Deputado tem 3 meses para apresentar a nota
        curr_month = today.month
        three_months_back = today - timedelta(days=90)
        three_months_urls = []
        for id in deputados_ids:
            for month in range(three_months_back.month, curr_month + 1):
                three_months_urls.append(f"deputados/{id}/despesas?ano={today.year}&mes={month}&itens=1000")
        return three_months_urls

if __name__ == "__main__":
    leg = {"dados": [{"id": 57, "uri": "https://dadosabertos.camara.leg.br/api/v2/legislaturas/57", "dataInicio": "2023-02-01", "dataFim": "2027-01-31", "anosPassados": [2023, 2024, 2025]}], "links": [{"rel": "self", "href": "https://dadosabertos.camara.leg.br/api/v2/legislaturas?data=2025-11-23"}, {"rel": "first", "href": "https://dadosabertos.camara.leg.br/api/v2/legislaturas?data=2025-11-23&pagina=1&itens=15"}, {"rel": "last", "href": "https://dadosabertos.camara.leg.br/api/v2/legislaturas?data=2025-11-23&pagina=1&itens=15"}]}
    print(urls_despesas([666], date(2025,2,1), leg))