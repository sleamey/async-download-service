import argparse
import asyncio
import os
import logging

from aiohttp import web
import aiofiles


async def archivate(request, photo_path: str = "./test_photos",
                    chunk_size: int = 4096, throttling_delay: float = 0, keep_broken_download: bool = False):
    response = web.StreamResponse()
    response.enable_chunked_encoding(chunk_size=chunk_size)
    response.headers['Content-Disposition'] = 'attachment; filename="archive.zip"'

    archive_hash = request.match_info['archive_hash']
    path = f"{photo_path}/{archive_hash}/"
    if not os.path.exists(path):
        logging.error(f"Путь {path} не существует!")
        raise web.HTTPNotFound(text="Сожалеем, но данный архив не найден, возможно он уже удален")

    # Отправляет клиенту HTTP заголовки
    await response.prepare(request)

    proc = await asyncio.create_subprocess_exec(
        "zip",  "-r", "-", path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    logging.info(f"Начинаем отправлять архив {archive_hash}")
    chunk_count = 0
    try:
        while True:
            archive_chunk = await proc.stdout.read(chunk_size)
            if not archive_chunk:
                logging.info(f"Отправка архива {archive_hash} завершена")
                return response
            logging.debug(f"Sending archive chunk #{chunk_count}")
            await response.write(archive_chunk)
            chunk_count += 1
            if throttling_delay:
                await asyncio.sleep(throttling_delay)

    except asyncio.CancelledError:
        logging.debug("Корутину попросили закругляться")
        if proc.returncode is None:  # Этой проверки недостаточно, но исключим заведомо закончившиеся процессы
            logging.debug(f"Останавливаем процесс архивации {archive_hash}")
            try:
                proc.terminate()
                await proc.communicate()
            except ProcessLookupError:  # без ограничений скорости процесс убивается кем-то еще
                logging.warning("Процесс архивации уже был закрыт кем-то до нас")

        if response.keep_alive:
            logging.debug("Закрываем соединение")
            response.force_close()
        raise
    finally:
        if keep_broken_download:
            logging.debug("Возвращаем все что есть клиенту")
            return response


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


def create_argparser():
    parser = argparse.ArgumentParser(description="Service for packing photos and streaming it to client on the fly")
    parser.add_argument('photo_path', help="Base path to stored photos")
    parser.add_argument('-p', '--port', default=8080, type=int, help="Listening port")
    parser.add_argument('-l', '--debug-logging', action='store_true', help="Enable verbose debug logging")
    parser.add_argument('-d', '--throttling-delay', default=0, type=float, help="Set server response delay")
    parser.add_argument('-c', '--chunk-size', default=4096, type=int, help="Set streaming response chunk size")
    parser.add_argument('-k', '--keep-broken-download', action='store_true',
                        help="Gracefully close connection on server shutdown to keep partially downloaded file")
    return parser


if __name__ == '__main__':
    arguments = create_argparser().parse_args()
    archivate_hanlder = lambda request: archivate(
        request, photo_path=arguments.photo_path, chunk_size=arguments.chunk_size,
        throttling_delay=arguments.throttling_delay, keep_broken_download=arguments.keep_broken_download
    )
    log_level = logging.DEBUG if arguments.debug_logging else logging.INFO
    logging.basicConfig(format="%(levelname)-8s [%(asctime)s] %(message)s", level=log_level)

    app = web.Application()
    app.add_routes([
        web.get('/', handle_index_page),
        web.get(r'/archive/{archive_hash:\w+}/', archivate_hanlder),
    ])
    web.run_app(app, port=arguments.port)

