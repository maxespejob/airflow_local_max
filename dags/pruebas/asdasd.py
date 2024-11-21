from datetime import datetime, timedelta
import calendar

# Obtener el primer y último día del mes actual
first_day = datetime.today().replace(day=1)
last_day = first_day + timedelta(days=calendar.monthrange(first_day.year, first_day.month)[1] - 1)

# Obtener el primer día del mes anterior
first_day_past_month = first_day - timedelta(days=1)
first_day_past_month = first_day_past_month.replace(day=1)

# Obtener el último día del mes anterior
last_day_past_month = first_day.replace(day=1) - timedelta(days=1)

# Resultados en formato 'YYYY-MM-DD'
print(first_day)
print(last_day)
print("Primer día del mes anterior:", first_day_past_month.strftime('%Y-%m-%d'))
print("Último día del mes anterior:", last_day_past_month.strftime('%Y-%m-%d'))
