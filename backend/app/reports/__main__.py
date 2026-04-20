import asyncio

from app.db import async_session
from app.reports.strategy_review import generate_default_strategy_review


async def _main() -> None:
    async with async_session() as session:
        result = await generate_default_strategy_review(session)
        print(result["review_path"])
        print(result["review_json_path"])
        print(result["analysis_path"])


if __name__ == "__main__":
    asyncio.run(_main())
