import asyncio
import time

import httpx

RANGE = 1000

requests_made = 0


async def start_timer():
    global requests_made
    seconds = 1
    while True:
        await asyncio.sleep(1)
        print(f"Requests made in {seconds} seconds: {requests_made}")
        requests_made = 0


async def send_request(client: httpx.AsyncClient) -> int:
    global requests_made
    # url = "https://dadosabertos.camara.leg.br/api/v2/deputados/204379"
    url = "https://legis.senado.leg.br/dadosabertos/senador/5322?v=6"
    print("Sending request")
    response = await client.get(url)
    print("Response recieved")
    requests_made += 1

    return response.status_code


async def main() -> int:
    asyncio.create_task(start_timer())

    start = time.perf_counter()
    
    queue = asyncio.Queue()

    limits = httpx.Limits(max_keepalive_connections=90, max_connections=90)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [asyncio.create_task(send_request(client)) for _ in range(RANGE)]
        status_codes = await asyncio.gather(*tasks)

    end = time.perf_counter()

    print(f"All work done in {end - start} seconds")

    return 0 if all(c == 200 for c in status_codes) else 1


if __name__ == "__main__":
    asyncio.run(main())
