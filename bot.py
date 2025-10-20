import asyncio
import logging
import io
import csv
import os
from typing import List, Optional
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, BufferedInputFile
from aiogram.filters import Command
from aiogram.exceptions import TelegramNetworkError, TelegramBadRequest

import organic_ya
import xmltree

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
BOT_TOKEN = os.environ.get('TOKENBOT')

if not BOT_TOKEN:
    raise RuntimeError("TOKENBOT не найден в .env файле!")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Константы
MAX_QUERIES = 10000  # Ограничение на количество запросов
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
SLEEP_BETWEEN_REQUESTS = 0.1  # Увеличена задержка для стабильности

@dp.message(Command(commands=['start']))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    await message.answer(
        "🔍 Привет! Я помогу собрать позиции сайтов в поисковой выдаче.\n\n"
        "📝 Отправьте мне:\n"
        "• Текстовое сообщение с запросами (каждый с новой строки)\n"
        "• Текстовый файл .txt с запросами\n\n"
        "📊 Получите CSV файл с результатами!"
    )

async def process_queries(queries: List[str], message: Message) -> Optional[bytes]:
    """
    Обработка списка поисковых запросов
    
    Args:
        queries: Список поисковых запросов
        message: Сообщение для отправки обновлений статуса
        
    Returns:
        CSV данные в виде bytes или None при ошибке
    """
    if len(queries) > MAX_QUERIES:
        await message.reply(f"❌ Слишком много запросов! Максимум {MAX_QUERIES}, получено {len(queries)}")
        return None

    try:
        a = organic_ya.Organic()
        output = io.StringIO()
        writer = None
        header_written = False
        
        total_queries = len(queries)
        processed = 0

        for i, query in enumerate(queries, 1):
            if not query.strip():
                continue
                
            try:
                logger.info(f"Обработка запроса {i}/{total_queries}: {query}")
                
                # Получаем XML данные
                xml_data = a.search_xmlriver(query.strip())
                
                # Записываем заголовок только один раз
                if not header_written:
                    header = xmltree.XmlTree.get_header(xml_data)
                    writer = csv.writer(output)
                    writer.writerow(header)
                    header_written = True

                # Обрабатываем данные и записываем строку
                b = xmltree.XmlTree(xml_data, query.strip())
                row = b.get_row()
                writer.writerow(row)
                processed += 1
                
                # Обновляем статус каждые 10 запросов
                if i % 10 == 0:
                    try:
                        await message.edit_text(f"⏳ Обработано {i}/{total_queries} запросов...")
                    except TelegramBadRequest:
                        # Игнорируем ошибку, если сообщение не изменилось
                        pass
                
                # Задержка между запросами
                await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)
                
            except Exception as e:
                logger.error(f"Ошибка при обработке запроса '{query}': {e}")
                # Продолжаем обработку других запросов
                continue

        if processed == 0:
            await message.reply("❌ Не удалось обработать ни одного запроса")
            return None

        # Конвертируем в bytes
        csv_content = output.getvalue()
        output.close()
        
        if not csv_content:
            return None
            
        return csv_content.encode('utf-8-sig')  # BOM для корректного отображения в Excel
        
    except Exception as e:
        logger.error(f"Общая ошибка при обработке запросов: {e}")
        await message.reply(f"❌ Произошла ошибка при обработке: {str(e)}")
        return None

async def extract_queries_from_text(text: str) -> List[str]:
    """Извлечение запросов из текста"""
    if not text:
        return []
    
    queries = []
    for line in text.strip().splitlines():
        line = line.strip()
        if line and not line.startswith('#'):  # Игнорируем комментарии
            queries.append(line)
    
    return queries

async def extract_queries_from_file(message: Message) -> Optional[List[str]]:
    """Извлечение запросов из файла"""
    if not message.document:
        return None
    
    # Проверяем размер файла
    if message.document.file_size and message.document.file_size > MAX_FILE_SIZE:
        await message.reply(f"❌ Файл слишком большой! Максимум {MAX_FILE_SIZE // (1024*1024)}MB")
        return None
    
    # Проверяем тип файла
    if message.document.mime_type not in ['text/plain', 'application/octet-stream']:
        await message.reply("❌ Пожалуйста, отправьте текстовый файл (.txt)")
        return None
    
    try:
        # Скачиваем файл
        file_info = await bot.get_file(message.document.file_id)
        buffer = io.BytesIO()
        await bot.download_file(file_info.file_path, buffer)
        buffer.seek(0)

        # Пробуем разные кодировки
        encodings = ['utf-8', 'utf-8-sig', 'windows-1251', 'cp1251']
        content = None
        
        for encoding in encodings:
            try:
                buffer.seek(0)
                content = buffer.read().decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            await message.reply("❌ Не удалось прочитать файл. Проверьте кодировку.")
            return None

        return await extract_queries_from_text(content)
        
    except Exception as e:
        logger.error(f"Ошибка при чтении файла: {e}")
        await message.reply("❌ Ошибка при чтении файла")
        return None

@dp.message(F.content_type.in_({'text', 'document'}))
async def handle_message(message: Message):
    """Основной обработчик сообщений"""
    try:
        queries = []

        # Обработка текстового сообщения
        if message.text and not message.text.startswith('/'):
            queries = await extract_queries_from_text(message.text)
            
        # Обработка файла
        elif message.document:
            queries = await extract_queries_from_file(message)
            if queries is None:
                return

        # Проверяем, есть ли запросы
        if not queries:
            await message.reply(
                "❌ Не найдено ни одного запроса!\n\n"
                "📝 Отправьте запросы в одном из форматов:\n"
                "• Текст с запросами (каждый с новой строки)\n"
                "• Текстовый файл .txt с запросами"
            )
            return

        # Информируем пользователя
        status_message = await message.answer(
            f"⏳ Начинаю обработку {len(queries)} запросов...\n"
            f"Это может занять {len(queries) * SLEEP_BETWEEN_REQUESTS / 60:.1f} минут"
        )

        # Обрабатываем запросы
        csv_data = await process_queries(queries, status_message)
        
        if csv_data is None:
            return

        # Отправляем результат
        try:
            await status_message.edit_text("📤 Отправляю результат...")
            
            input_file = BufferedInputFile(
                csv_data,
                filename=f"search_results_{len(queries)}_queries.csv"
            )
            
            await message.answer_document(
                document=input_file,
                caption=f"✅ Готово! Обработано {len(queries)} запросов"
            )
            
            # Удаляем статусное сообщение
            try:
                await status_message.delete()
            except:
                pass
                
        except TelegramNetworkError as e:
            logger.error(f"Ошибка сети Telegram: {e}")
            await message.reply(
                "❌ Ошибка при отправке файла. Возможно, файл слишком большой или проблемы с сетью.\n"
                "Попробуйте разбить запросы на меньшие группы."
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке документа: {e}")
            await message.reply("❌ Ошибка при отправке результата")

    except Exception as e:
        logger.error(f"Общая ошибка в handle_message: {e}")
        await message.reply("❌ Произошла неожиданная ошибка")

@dp.message()
async def handle_other(message: Message):
    """Обработчик прочих сообщений"""
    await message.answer(
        "🤔 Не понимаю эту команду.\n"
        "Отправьте /start для получения инструкций."
    )

async def main():
    """Главная функция"""
    logger.info("Запуск бота...")
    
    try:
        # Удаляем webhook если был установлен
        await bot.delete_webhook(drop_pending_updates=True)
        
        # Запускаем polling
        await dp.start_polling(
            bot,
            skip_updates=True,  # Пропускаем старые обновления
            allowed_updates=["message"]  # Обрабатываем только сообщения
        )
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        await bot.session.close()
        logger.info("Бот остановлен")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Ошибка запуска: {e}")
