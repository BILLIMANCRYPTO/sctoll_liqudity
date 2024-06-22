import requests
import random
import time
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from fake_useragent import UserAgent

# Протоколы для запроса
protocols = [
    'scrl_syncswap',
    'scrl_zebra',
    'scrl_izumi',
    'scrl_nuri',
    'scrl_ambient',
    'scrl_cog',
    'scrl_aave3',
    'scrl_rhomarkets',
    'scrl_compound3',
    'scrl_lineabank'
]


# Функция чтения прокси из файла
def read_proxies():
    with open('proxies.txt', 'r') as f:
        proxies = [line.strip() for line in f]
    return proxies


# Функция получения случайного прокси
def get_random_proxy(proxies):
    if not proxies:
        raise ValueError("Список прокси пуст")
    proxy_index = random.randint(0, len(proxies) - 1)
    proxy = proxies[proxy_index]
    ip, port, user, password = proxy.split(':')
    return {
        'http': f'socks5://{user}:{password}@{ip}:{port}',
        'https': f'socks5://{user}:{password}@{ip}:{port}'
    }


# Функция для запроса данных и извлечения asset_usd_value
def fetch_protocol_value(wallet_address, protocol, proxies, user_agent):
    retry_delay = 180  # задержка в секундах (3 минуты)
    for _ in range(len(proxies)):
        proxy = get_random_proxy(proxies)
        url = f'https://api.rabby.io/v1/user/protocol?id={wallet_address}&protocol_id={protocol}'
        headers = {'User-Agent': user_agent.random}
        try:
            response = requests.get(url, proxies=proxy, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                asset_usd_value = sum(
                    item.get('stats', {}).get('asset_usd_value', 0) for item in data.get('portfolio_item_list', []))
                return asset_usd_value
            elif response.status_code == 429:
                print(f"Лимит запросов достигнут для {url}, ожидание {retry_delay // 60} минут")
                time.sleep(retry_delay)
                continue
            else:
                print(f"Неверный статус код {response.status_code} для {url}")  # Логирование статуса
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе {url}: {e}")
            continue
    return 0


# Функция для обработки кошелька
def process_wallet(index, wallet_address, proxies, user_agent):
    wallet_result = {'index': index + 1, 'wallet_address': wallet_address}
    total_value = 0.0

    for protocol in protocols:
        value = fetch_protocol_value(wallet_address, protocol, proxies, user_agent)
        wallet_result[protocol] = value
        total_value += value

    wallet_result['total_liquidity'] = total_value
    return wallet_result


# Чтение адресов кошельков из файла wallets.txt
with open('wallets.txt', 'r') as file:
    wallet_addresses = file.readlines()

# Удаление пробельных символов из адресов
wallet_addresses = [address.strip() for address in wallet_addresses]

# Чтение прокси из файла proxies.txt
proxies = read_proxies()

# Инициализация fake_useragent
user_agent = UserAgent()

# Использование ThreadPoolExecutor для многопоточности
results = [None] * len(wallet_addresses)
with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {executor.submit(process_wallet, index, wallet_address, proxies, user_agent): index for
               index, wallet_address in
               enumerate(wallet_addresses)}
    for future in tqdm(as_completed(futures), total=len(wallet_addresses), desc="Обработка кошельков"):
        index = futures[future]
        try:
            result = future.result()
            results[index] = result
        except Exception as exc:
            print(f'Кошелек {wallet_addresses[index]} вызвал исключение: {exc}')

# Переименование столбцов
new_column_names = {
    'index': 'Index',
    'wallet_address': 'Wallet Address',
    'total_liquidity': 'Total Liquidity',
    'scrl_syncswap': 'SyncSwap',
    'scrl_zebra': 'Zebra',
    'scrl_izumi': 'Izumi',
    'scrl_nuri': 'Nuri',
    'scrl_ambient': 'Ambient',
    'scrl_cog': 'CogFinance',
    'scrl_aave3': 'Aave',
    'scrl_rhomarkets': 'Rho Markets',
    'scrl_compound3': 'Compound',
    'scrl_lineabank': 'LayerBank'
}

# Преобразование результатов в DataFrame и переименование столбцов
df = pd.DataFrame(results)
df.rename(columns=new_column_names, inplace=True)

# Определение нового порядка столбцов
column_order = ['Index', 'Wallet Address', 'Total Liquidity'] + \
               [new_column_names[protocol] for protocol in protocols]

# Переупорядочивание столбцов в DataFrame
df = df[column_order]

# Запись данных в Excel файл с применением стилей к заголовкам и выравниванием по центру
excel_file = "wallet_results.xlsx"
with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
    df.to_excel(writer, index=False, sheet_name='Results')
    workbook = writer.book
    worksheet = writer.sheets['Results']

    # Применение стилей к заголовкам
    header_format = workbook.add_format({
        'bold': True,
        'font_size': 16,
        'align': 'center',
        'valign': 'vcenter'
    })

    # Автоматическая подгонка ширины столбцов и применение стиля к заголовкам
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
        max_len = max(df[value].astype(str).map(len).max(), len(value))
        worksheet.set_column(col_num, col_num, max_len + 2)

print("Excel файл 'wallet_results.xlsx' создан успешно.")
