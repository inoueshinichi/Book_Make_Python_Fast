import asyncio

async def lazy_printer(delay, message):
    await asyncio.sleep(delay)
    print(message)

async def main():
    asyncio.wait([
        await lazy_printer(1, "I am lazy"), 
        await lazy_printer(0, "Full Speed")
        ],
        timeout=None,
        return_when="ALL_COMPLETED",
        )

asyncio.run(main())