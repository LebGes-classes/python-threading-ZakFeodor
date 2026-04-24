import excel_work_async
import excel_work_threads
import asyncio

print(excel_work_threads.main())
print(asyncio.run(excel_work_async.main()))
