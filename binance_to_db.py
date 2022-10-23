import asyncio
from binance import AsyncClient, BinanceSocketManager
import clickhouse_connect


def insert_data_to_clickhouse(data:dict):
    # push data to clickhouse db
    query_statement = "INSERT INTO cryptodatabase.cryptoprice (*) values ('{}',{},'{}',{},{},{},{},{},{},'{}','{}')".format(data['e'], data['E'], data['s'], data['t'],  data['p'], data['q'], data['b'], data['a'], data['T'], data['m'], data['M'])
    client = clickhouse_connect.get_client(host='127.0.0.1', port=8123, username='default', password='123456')
    client.query(query_statement)

async def main():
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client,user_timeout=60)
    # start any sockets here, i.e a trade socket
    # ts = bm.trade_socket('BTCBUSD')
    ms = bm.multiplex_socket(['btcbusd@trade', 'ethbusd@trade'])
    # then start receiving messages
    async with ms as mscm:
        while True:
            res = await mscm.recv()
            print(res)
            data = res['data']
            insert_data_to_clickhouse(data)

    await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())