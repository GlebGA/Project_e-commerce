#!/usr/bin/env python
# coding: utf-8

# # Проект e-commerce.

# Продакт-менеджер попросил вас проанализировать совершенные покупки и ответить на следующие вопросы:
# 
# 1. Сколько у нас пользователей, которые совершили покупку только один раз?
# 
# 2. Сколько заказов в месяц в среднем не доставляется по разным причинам (вывести детализацию по причинам)?
# 
# 3. По каждому товару определить, в какой день недели товар чаще всего покупается.
# 
# 4. Сколько у каждого из пользователей в среднем покупок в неделю (по месяцам)? Не стоит забывать, что внутри месяца может быть не целое количество недель. Например, в ноябре 2021 года 4,28 недели. И внутри метрики это нужно учесть.
# 
# 5. Используя pandas, проведи когортный анализ пользователей. В период с января по декабрь выяви когорту с самым высоким retention на 3й месяц.Описание подхода можно найти тут.
# 
# 6. Часто для качественного анализа аудитории использую подходы, основанные на сегментации. Используя python, построй RFM-сегментацию пользователей, чтобы качественно оценить свою аудиторию. В кластеризации можешь выбрать следующие метрики: R - время от последней покупки пользователя до текущей даты, F - суммарное количество покупок у пользователя за всё время, M - сумма покупок за всё время. Подробно опиши, как ты создавал кластеры. Для каждого RFM-сегмента построй границы метрик recency, frequency и monetary для интерпретации этих кластеров. Пример такого описания: RFM-сегмент 132 (recency=1, frequency=3, monetary=2) имеет границы метрик recency от 130 до 500 дней, frequency от 2 до 5 заказов в неделю, monetary от 1780 до 3560 рублей в неделю. Описание подхода можно найти тут. 

# In[680]:


import pandas as pd
import numpy as np
import seaborn as sns
import calendar
from datetime import datetime
from dateutil import relativedelta


# In[681]:


customers = pd.read_csv('olist_customers_dataset.csv')


# In[682]:


orders    = pd.read_csv('olist_orders_dataset.csv')


# In[683]:


items     = pd.read_csv('olist_order_items_dataset.csv')


# In[684]:


customers.head()


# In[685]:


orders.head()


# In[686]:


items.head()


# ## 1. Сколько у нас пользователей,  которые совершили покупку только один раз?
#   
#   

# А что же считать за совершенную покупку? с моей точки зрения факт совершения покупки - оплата заказа, поэтому высчитываю количество пользователей по колонке времени подтверждения оплаты заказа

#   
# Для этого объединяю датафреймы customers и orders по общей колонке customer_id

# In[687]:


cust_and_order = customers.merge(orders, how = 'inner', on = 'customer_id')


# In[688]:


#Удаляю строки, где отсутствует подтверждение оплаты
cust_and_order['order_approved_at'].replace('', np.nan, inplace=True)
cust_and_order.dropna(subset=['order_approved_at'], inplace=True)
cust_and_order.shape


# In[689]:


#Группирую по уникальному идентификатору пользователя, 
#считаю количество заказов на пользователя и фильтрую по нужному количеству заказов (1)
cust_on_one_order = cust_and_order.groupby('customer_unique_id', as_index = False)                                   .agg({'order_id' : 'count',})                                   .query('order_id == 1')


# In[690]:


cust_on_one_order.shape


# ### В результате видим что количество клиентов, совершивших покупку только один раз равно 93049

#   

# ## 2. Сколько заказов в месяц в среднем не доставляется по разным причинам (вывести детализацию по причинам)?

# In[691]:


#Преобразую все даты к правильному типу данных
orders['order_purchase_timestamp']      = pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_approved_at']             = pd.to_datetime(orders['order_approved_at'])
orders['order_delivered_carrier_date']  = pd.to_datetime(orders['order_delivered_carrier_date'])
orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])


# In[692]:


#Создам колонку с временем от даты создания заказа до получения доставки клиентом
orders['time_to_cust'] = orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']


# In[693]:


orders.time_to_cust.mean() ## среднее время доставки


# In[694]:


#отберу те заказы, где клиент не получил заказ
orders_not_delivery = orders.query("order_delivered_customer_date == ''")


# In[695]:


orders_not_delivery.order_purchase_timestamp.max() # время создания последнего заказа из ДФ


# In[696]:


orders_not_delivery.order_purchase_timestamp.min() # время создания первого заказа из ДФ


# In[697]:


#расчитаю количество месяцев между заказами
date_first = datetime.strptime('2016-09-04 21:15:19', '%Y-%m-%d %H:%M:%S')
date_end   = datetime.strptime('2018-10-17 17:30:18', '%Y-%m-%d %H:%M:%S')
r          = relativedelta.relativedelta(date_end, date_first)
r.months + (12*r.years)


# In[698]:


#отберу заказы, для ровного счета количества заказов за 25 месяцев плюс последний заказ будет сделан за 14 дней
#до конца ДФ с данными, поэтому их в среднем тоже должны были доставить т.к. среднее время доставки 12 дней
orders_not_delivery = orders_not_delivery.query("order_purchase_timestamp <= '2018-09-04 21:15:19'")
orders_not_delivery.order_purchase_timestamp.max()


# In[699]:


orders_not_delivery.shape


# ### Получется, что до клиентов за 25 месяцев работы магазина не доехало 2949 заказа

# In[700]:


2949/25


# ### Или примерно 118 заказов в месяц

# #### А почему заказ не доставлен?

# In[701]:


# создаю отсчетную дату - последнюю дату заказа в ДФ
orders_not_delivery['finish_time'] = pd.to_datetime('2018-09-03 18:45:34')


# In[702]:


# показываю сколько прошло времени от обещанного времени доставки до конца исследуемого ДФ
orders_not_delivery['delivery_time'] = orders_not_delivery['finish_time'] - orders_not_delivery['order_estimated_delivery_date']


# In[703]:


orders_not_delivery['order_status'].value_counts()


# In[704]:


# отбираю по первому статусу - отгружен со склада, фильтрую по времени и отбираю те, которые еще могут быть доставлены вовремя
orders_not_shipped = orders_not_delivery.query("order_status == 'shipped'").sort_values('order_estimated_delivery_date').query("delivery_time > '0 days +18:45:34'")


# In[705]:


orders_not_shipped.shape


# In[706]:


orders_not_shipped.delivery_time.mean()


# 1) статус заказа: отгружен со склада, то есть заказ передан в логистическую службу, и не доставлен по какой-то причине клиенту в срок, таких клиентов 1103 и среднее время задержки 247 дней, а максимальное время задержки 683 дня, что очень странно, почему заказ не переведен до сих пор в статус отменен? С моей точки зрения не доработана взаимосвязь между продавцом и логистической компанией, товары вполне могут быть доставлены, но статусы не обновлены. 

# In[707]:


orders_unavailable = orders_not_delivery.query("order_status == 'unavailable'").sort_values('order_estimated_delivery_date')#.query("delivery_time > '0 days +18:45:34'")


# In[708]:


orders_unavailable.query("order_delivered_carrier_date == ''")


# 2) Статус заказа - недоступен, то есть магазин принял оплату, пообещал дату доставки, но не смог передать заказ в логистическую службу, с моей точки зрения опять же недоработка на автоматизированном уровне со службой доставки, по логике сайт продавца автоматически расчитывает и обещает дату доставки, а в последствии логистическая служба отказывает продавцу и заказ получает статус "недоступен" (что-то вроде "доставка для вашего региона не доступна"), а так же это может быть отсутсвие товара на складе, в общем данный статус - отмена со стороны продавца

# In[709]:


orders_canceled = orders_not_delivery.query("order_status == 'canceled'").sort_values('order_estimated_delivery_date').query("delivery_time > '0 days +18:45:34'")


# In[710]:


orders_canceled.query("order_delivered_carrier_date != ''")


# In[711]:


orders_canceled.query("order_delivered_carrier_date != ''").shape


# 3) статус заказа - отменен, здесь как раз с моей точки зрения - отмена заказа со стороны покупателя, часть заказов даже была передана в службу доставки, но все равно в итоге отменена

# In[712]:


orders_invoiced = orders_not_delivery.query("order_status == 'invoiced'").sort_values('order_estimated_delivery_date')


# In[713]:


orders_invoiced


# 4) статус заказа - выставлен счет, однако во всех заказах данной выборки стоит время подтверждения оплаты, поэтому с моего точки зрения здесь какая-то техническая ошибка в статусах: все данные заказы должны быть в других статусах

# In[714]:


orders_processing = orders_not_delivery.query("order_status == 'processing'").sort_values('order_estimated_delivery_date')


# In[715]:


orders_processing


# 5) Статус заказ - в процессе сборки заказа, опять же не понятный статус заказа, некоторым заказам почти по два года и они до сих пор в процессе

# In[716]:


orders_delivered = orders_not_delivery.query("order_status == 'delivered'")


# In[717]:


orders_delivered


# 6) статус заказа - доставлен, хотя не стоит времени доставки заказа - здесь опять же может быть ряд причин, как техническая ошибка, недоработка связи с логистической компанией или ошибка внутреннего персонала магазина продавца

# In[718]:


orders_created = orders_not_delivery.query("order_status == 'created'")


# In[719]:


orders_created


# 7) статус заказа - создан и не оплачен, ничего странного здесь нет

# In[720]:


orders_approved = orders_not_delivery.query("order_status == 'approved'")


# In[721]:


orders_approved


# 8) статус заказа - подтвержден, два оплаченных заказа, которые должны были перейти в статус "в процессе сборки", но по какой-то причине остались здесь. В общем очень много вопросов к менеджеру магазина...

# ### 3. По каждому товару определить, в какой день недели товар чаще всего покупается.

# In[722]:


items_in_orders = items.merge(orders, how = 'inner', on = 'order_id') #соединяю ДФ


# In[723]:


items_in_orders['order_approved_at'].replace('', np.nan, inplace=True)
items_in_orders.dropna(subset=['order_approved_at'], inplace=True) 
#Удаляю строки, где отсутствует подтверждение оплаты


# In[724]:


items_in_orders['weekday_max_pay'] = items_in_orders.order_approved_at.dt.weekday
#создаю колонку с днем недели оплаты


# In[725]:


items_in_orders


# In[726]:


days = {0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}

items_in_orders['weekday_max_pay'] = items_in_orders['weekday_max_pay'].apply(lambda x: days[x]) #переименовываю ее в приятный вид


# In[727]:


items_in_orders


# In[728]:


max_pay = items_in_orders.groupby([ 'product_id', 'weekday_max_pay'], as_index = False)                          .agg({'order_purchase_timestamp' : 'count'})                          .sort_values('order_purchase_timestamp', ascending=False)                          .drop_duplicates(subset=['product_id'])                          .drop('order_purchase_timestamp', axis=1)

#группирую по ID товара, аггрегирую и фильтрую по количеству покупок за день недели, 
#убираю дубликаты (оставляю только самое большое количество покупок за день),
#убираю столбец с количеством покупок и остается два столбца - ID товара и день недели,
#в который его купили большинство раз что и требовалось найти


# In[729]:


max_pay


# ### 4. Сколько у каждого из пользователей в среднем покупок в неделю (по месяцам)? Не стоит забывать, что внутри месяца может быть не целое количество недель. Например, в ноябре 2021 года 4,28 недели. И внутри метрики это нужно учесть.

# In[730]:


customers_and_order = customers.merge(orders, how = 'inner', on = 'customer_id') #соединяю ДФ


# In[731]:


customers_and_order.shape


# In[732]:


customers_and_order['order_approved_at'].replace('', np.nan, inplace=True)
customers_and_order.dropna(subset=['order_approved_at'], inplace=True)  # удаляю строки без подтверждения оплаты (покупка не подтверждена)


# In[733]:


customers_and_order.shape


# In[734]:


#создаю колонку с количеством недель в месяце и преобразую колонку подтверждения оплаты в дату
customers_and_order['weeks_in_month']    = customers_and_order.order_approved_at.dt.daysinmonth / 7
customers_and_order['order_approved_at'] = pd.to_datetime(customers_and_order['order_approved_at'])
customers_and_order['month_year_pay']    = customers_and_order['order_approved_at'].apply(lambda x:x.strftime('%b - %Y'))


# In[735]:


customers_and_order


# In[736]:


#группирую и аггрегирую
pay_in_month_year = customers_and_order .groupby(['customer_unique_id', 'weeks_in_month', 'month_year_pay'], as_index = False)                                         .agg({'order_id' : 'count'})                                         .sort_values('order_id')                                         .rename(columns = {'order_id' : 'count_order'})


# In[737]:


#определяю сколько у каждого из пользователей в среднем покупок в неделю (по месяцам), что и требовалось найти
pay_in_month_year['pays_in_week'] = pay_in_month_year['count_order'] / pay_in_month_year['weeks_in_month']


# In[738]:


pay_in_month_year


# ### 5. Используя pandas, проведи когортный анализ пользователей. В период с января по декабрь выяви когорту с самым высоким retention на 3й месяц.

# In[739]:


# соединяю ДФ и фильтруем заказы без подтверждения оплаты
custom_order = customers.merge(orders, on = 'customer_id')
custom_order = custom_order[['customer_id', 'customer_unique_id', 'order_id', 'order_approved_at']]
custom_order = custom_order.fillna(0)
custom_order = custom_order.query('order_approved_at != 0')


# In[740]:


# перевожу в дату
custom_order['order_approved_at'] = pd.to_datetime(custom_order['order_approved_at'])
custom_order['order_approved_at'] = custom_order.order_approved_at.dt.strftime('%Y-%m')


# In[741]:


#создаю таблицу с датой первого заказа
first_orders = custom_order.groupby('customer_unique_id', as_index = False)                                   .agg({'order_approved_at' : 'min'})                                   .rename(columns = {'order_approved_at' : 'first_order'})


# In[742]:


first_orders


# In[743]:


#подсчитываю количество первых покупок в месяц
first_orders_count = first_orders.groupby('first_order', as_index = False)                                  .agg({'customer_unique_id' : 'count'})                                  .rename(columns = {'customer_unique_id' : 'first_orders_cnt'})


# In[744]:


first_orders_count


# In[745]:


#мержу ДФ
order_merged = custom_order.merge(first_orders, on = 'customer_unique_id')


# In[746]:


order_merged = order_merged.merge(first_orders_count, on = 'first_order')


# In[747]:


order_merged


# In[748]:


orders_in_month = order_merged.groupby(['customer_unique_id', 'first_order', 'order_approved_at'], as_index = False)                               .agg({'customer_id' : 'count'})


# In[749]:


orders_in_month


# In[750]:


#создаю таблицу с группировкой по первым покупкам и покупкой тех же когорт в последствии
orders_2_month = orders_in_month.groupby(['first_order', 'order_approved_at'], as_index = False)                               .agg({'customer_id' : 'count'})


# In[751]:


orders_2_month


# In[752]:


orders_2_month_merge = orders_2_month.merge(first_orders_count, on = 'first_order')


# In[753]:


# создаю колонку с расчетом удержания (retention)
orders_2_month_merge ['retention_pr'] = (orders_2_month_merge ['customer_id'] / orders_2_month_merge ['first_orders_cnt'] * 100).round(2)


# In[754]:


orders_2_month_merge


# In[755]:


#делаю пивот , где индексы - даты первых покупок, колонки - даны последующих, и наполнение - процент удержания
retention = orders_2_month_merge.pivot(index = 'first_order', columns = 'order_approved_at', values = 'retention_pr').fillna(0)


# In[756]:


retention = retention.drop(labels = ['2016-09','2016-10','2016-12', '2018-06', '2018-07', '2018-08'], axis = 0)
retention = retention.drop(labels = ['2016-09','2016-10','2016-12', '2018-06', '2018-07', '2018-08', '2018-09'], axis = 1)


# In[757]:


retention


# In[ ]:





# ##### В итоге видно, что клиенты, совершившие покупку в первый раз в июне 2017 имеют самую большую когорту на третий месяц (сентябрь - 0.41)
# ##### но
# ##### что если третий месяц включая месяц покупки? тогда получается что клиенты, совершившие покупку в первый раз в мае 2017 имеют самую большую когорту на третий месяц (июль - 0.50)

# #### 6. Часто для качественного анализа аудитории использую подходы, основанные на сегментации. Используя python, построй RFM-сегментацию пользователей, чтобы качественно оценить свою аудиторию. В кластеризации можешь выбрать следующие метрики: R - время от последней покупки пользователя до текущей даты, F - суммарное количество покупок у пользователя за всё время, M - сумма покупок за всё время. Подробно опиши, как ты создавал кластеры. Для каждого RFM-сегмента построй границы метрик recency, frequency и monetary для интерпретации этих кластеров. Пример такого описания: RFM-сегмент 132 (recency=1, frequency=3, monetary=2) имеет границы метрик recency от 130 до 500 дней, frequency от 2 до 5 заказов в неделю, monetary от 1780 до 3560 рублей в неделю.

# In[762]:


#создаю общей ДФ
orders = orders.drop('time_to_cust', axis=1)
all_data = customers.merge(orders, on = 'customer_id')                     .merge(items,  on = 'order_id')


# In[763]:


#проверяю типы данных
all_data.dtypes


# In[764]:


#определяю количество пропущенных значений
all_data.isna().sum()


# In[765]:


#заменяю пропущенные значений нулем
all_data = all_data.fillna(0)


# In[766]:


#убираю заказы без подтверждения оплаты
all_data = all_data.query('order_approved_at != 0')


# In[767]:


# время создания последнего заказа из ДФ - будем считать текущей датой
orders.order_purchase_timestamp.max() 


# In[768]:


# перевожу факт оплаты в дату 
all_data['order_approved_at'] = pd.to_datetime(all_data['order_approved_at'])
all_data['order_approved_at'] = all_data.order_approved_at.dt.strftime('%Y-%m-%d')


# In[769]:


all_data


# In[770]:


#нахожу дату последней оплаты поклиентно
recency = all_data.groupby('customer_unique_id', as_index = False)                   .agg({'order_approved_at' : 'max'})


# In[771]:


#создаю колонку с текущей датой
recency['last_pay'] = '2018-10-17'


# In[772]:


recency


# In[773]:


recency.dtypes


# In[774]:


#для дальнейшей вычитания даты из даты мне понадобится тип дататайм
recency['order_approved_at'] = pd.to_datetime(recency['order_approved_at'])
recency['last_pay']          = pd.to_datetime(recency['last_pay'])


# In[775]:


#создаю колонку разницы между последней оплатой и текущей датой (датой последнего заказа)
recency['recency'] = recency['last_pay'] - recency['order_approved_at']


# In[776]:


recency


# In[777]:


#добавляю данные в основной ДФ


# In[778]:


all_data = all_data.merge(recency, on = 'customer_unique_id')


# In[779]:


all_data


# In[780]:


#далее создаю RFM таблицу
rfm = all_data.groupby('customer_unique_id', as_index = False)               .agg({'recency' : 'min', 'order_id' : 'count', 'price' : 'sum'})               .rename(columns = {'order_id' : 'frequency', 'price' : 'monetary'})


# In[781]:


rfm


# In[782]:


#перевожу recency в правильный вид
rfm.recency = rfm.recency.dt.days


# In[783]:


rfm


# In[784]:


#Сегментирую по квантилям - делю на три части
rfm_qua = rfm.quantile(q = [0.33, 0.66])
rfm_qua


# In[785]:


#создаю функции для рфм
def recency(x, p, d):
    if x < 196:
        return 3
    elif x >= 335:
        return 1
    else:
        return 2

def frequency(x, p, d):
    if x <= 1:
        return 1
    elif x > 1:
        return 3
    else:
        return 2

def monetary(x, p, d):
    if x < 57.99:
        return 1
    elif x >= 125.00:
        return 3
    else:
        return 2

rfm['R_segment'] = rfm.recency.apply(recency, args=('recency',rfm_qua))
rfm['F_segment'] = rfm.frequency.apply(frequency, args=('frequency',rfm_qua))
rfm['M_segment'] = rfm.monetary.apply(monetary, args=('monetary',rfm_qua))
rfm['RFM_class'] = rfm['R_segment'].map(str) + rfm['F_segment'].map(str) + rfm['M_segment'].map(str)


# In[786]:


#Сегментирование получилось:
#давность        R: 1 - давно,      2 - относительно недавно, 3 - недавно
#частота покупок F: 1 - один раз,   2 - редко,                3 - часто
#сумма покупок   M: 1 - низкий чек, 2 - средний чек,          3 - высокий чек


# In[ ]:





# In[787]:


#отберу лучших клиентов, RFM - 333
rfm.query("RFM_class == '333'").shape
#таких замечатальных клиентов получилось 2334


# In[788]:


#отберу клиентов которые максимально плохо покупали и редко, RFM - 111
rfm.query("RFM_class == '111'").shape
#таких клиентов больше - 10106


# In[789]:


#отберу клиентов которые покупали недавно, часто, но чек маленький, RFM - 331
rfm.query("RFM_class == '331'").shape
#таких клиентов - 478


# In[790]:


#отберу клиентов которые покупали относительно недавно, редко, но чек высокий, RFM - 331
rfm.query("RFM_class == '223'").shape
#таких клиентов - 0, таких нет


# Можно анализировать много вариантов, все зависит от целей.

# In[ ]:




