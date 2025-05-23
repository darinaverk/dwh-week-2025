# Очистка данных (слоя ODS)

Работа с данными в хранилище Яндекс маркета начинается задолго, до того момента, как они попадают в YT. Но давайте оставим за скобками работу с сервисами:
Система источник уже любезно складывает на YT события относящиеся к заказам:
//home/homework_task_dwh/order_event.
(Все данные являются синтетическими, но основаны на реальных)

Первую задачу которую вам предстоит решить в хранилище, это "очистка" данных:
Нужно собрать таблицу с информацией о событиях "позиций заказов"(Простым языком это товары, входящие в заказ пользователя).
Схема таблицы приложена ниже:

| \# | name            | type                |описание                       |
|----|-----------------|---------------------|-------------------------------|
| 1	 | item_id	       | Int64[optional]     |идентификатор внутри заказов   |
| 2	 | create_datetime | Datetime[optional]  |время создания события         |
| 3	 | cyr	           | String[optional]    |валюта покупки                 |
| 4	 | item_count	   | Int64[optional]     |кол-во позиций заказа в заказе |
| 5	 | item_price	   | Int64[optional]     |цена позиции заказа            |
| 6	 | order_id	       | Int64               |номер заказа                   |
| 7	 | order_status	   | String              |стаус заказа                   |
| 8	 | sku	           | Int64[optional]     |артикул позиции заказа         |
| 9	 | user_id	       | Int64               |идентификатор пользователя     |

Также схема таблицы есть в папке с задачей: //home/homework_task_dwh

>>> [!note] P.S.:
1. Для выполнения задания вам может понядобиться YQL — универсальный, декларативный, основанный на SQL язык запросов (документацию можно найти тут: https://ytsaurus.tech/docs/ru/yql/ );
2. Для запуска проверки, верните в функции ``get_table_path()`` путь до получившейся таблицы;
3. Обратите внимание, что из заказов могут "пропадать" позиции, используйте в таком случае статус "deleted";
4. Событии от источника приходят в гранулярности заказа, обратите внимание, что нам нужна гранулярность "позиция заказа" (каждый купленный товар отдельной строчкой);
5. Таблица должна быть отсортирована по item_id.
6. Уточнение: Большинство задач в DWH именно на исследование данных. Не хочется полностью убить эту важную составляющую: в реальных условиях нет даже подсказок, есть события, а дальше начинает самое интересное - анализ данных.
7. Обрабатывая сырые данные, мы стараемся не делать операции, которые теряют данные: поэтому тут нет агрегаций: для каждого item_id, для каждого события будет своя строка. Если у Вас получилось около 341 тысяч строк - Вы близки к победе.
8. Для подключения через CLI/API можно использовать прокси 158.160.140.68, но через YQL точно решение проще )
>>>

Удачи в выполнении первого задания!
