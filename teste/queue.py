import asyncio


async def cashier(queue, cashier_id):
    while True:
        customer = await queue.get()
        print(f"Cashier {cashier_id} is checking out {customer}")
        await asyncio.sleep(1)  # simulate checkout time
        print(f"Cashier {cashier_id} finished with {customer}")
        queue.task_done()


async def main():
    queue = asyncio.Queue()
    # Start 3 cashiers
    for i in range(3):
        asyncio.create_task(cashier(queue, i + 1))
    # Simulate 10 customers joining the line
    for i in range(10):
        await queue.put(f"Customer {i + 1}")
        await asyncio.sleep(0.1)  # slight delay in arrivals
    await queue.join()  # wait for all customers to be processed


asyncio.run(main())
