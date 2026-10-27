from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession


async def get_asyncpg_connection(bind: AsyncSession | AsyncConnection):
    if isinstance(bind, AsyncSession):
        sqlalchemy_connection = await bind.connection()
    else:
        sqlalchemy_connection = bind
    raw_connection = await sqlalchemy_connection.get_raw_connection()
    driver_connection = raw_connection.driver_connection
    if driver_connection is None:
        raise RuntimeError("无法获取 PostgreSQL 原生连接，请确认 DATABASE_URL 使用 postgresql+asyncpg")
    return driver_connection
