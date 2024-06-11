# -*- coding: utf-8 -*-
from run import Run
import read_files as rf


# Файлы с выгрузкой
f_assessment = r'C:\Users\i.tverdov\Documents\Дашборды\ЭР 4\Исходники\organization.xlsx'
f_os_stream = r'C:\Users\i.tverdov\Documents\Дашборды\ЭР 4\Исходники\ОС ЭР 4 поток.xlsx'
f_os_lesson = r'C:\Users\i.tverdov\Documents\Дашборды\ЭР 4\Исходники\ОС лекции ЭР 4 поток.xlsx'
f_progress = r'C:\Users\i.tverdov\Documents\Дашборды\ЭР 4\Исходники\Прогресс ЭР 4 поток.xlsx'

r = Run()

# Что нужно вводить в начале
#   - наименование программы, Номер потока
name, number_stream = 'Эффективный руководитель', 4
# Добавляем в каждый лист каждого файла выгрузки столбец "ФИО" путём сцепки фамилии, имени и отчествтва
# В файл с ОС по программе добавить две колонки: "Завершил" и "Итоговый балл"
# !!! ВАЖНО !!! Найден артефакт, кторый не позволяет открываться в df. Находится он в модулях, в "Форма обратной связи (Необходимо заполнить для перехода на следующий модуль). Форма обратной связи. Насколько вы удовлетворены материалами модуля?".
# Произвести замену и убрать все префиксы (Форма обратной связи (Необходимо заполнить для перехода на следующий модуль). Форма обратной связи. )


# 1 Создаем программу (True) или ищем из имеющихся (False)
new = True
id_programm = r.create_programm(name, new)

# 2. Создаём поток
id_stream = r.create_stream(number_stream, id_programm)

# 3. Считываем ФИО слушателей
df = rf.read_fios(f_os_stream)
r.insert_user(df, id_stream)

# 4. Читаем файл с ОС по программе
df = rf.read_os_stream(f_os_stream)

# 5. Добавляем feedback по всей программе
r.insert_feedback_programm(df)

# 6. Добавление результатов ассессмента
df = rf.read_assessment(f_assessment)
if new:
    r.insert_competence(df, id_programm)
r.insert_assessment(df, id_stream)

# 7. Читаем файл с ОС ещё раз для считывания 
dfs = rf.read_os_stream_moduls(f_os_stream)

# 8. Добавляем ОС по модулям
r.insert_feedback_module(dfs, id_programm)

# 9. Читаем файл "Прогресс" для выявления добавления тьюторов, если они есть на потоке
df = rf.read_for_totors(f_progress)
r.insert_tutor(df, id_stream)

# 10. Вносим результаты тестирования
dfs = rf.read_progress_testing(f_progress)
r.insert_testing(dfs, id_programm, id_stream)

# 11. Читаем файл с ОС по лекциями
dfs = rf.read_os_lesson(f_os_lesson)

# 12. Добавляем уроки и упражнения (Работа с файлом "ОС лекции")
if new:
    r.create_lesson(dfs, id_programm)
exrs = rf.read_progress_for_exrs(f_progress)
r.create_exsercise(exrs, id_programm)

# 13. Добавляем прогресс по материалам
dfs = rf.read_progress(f_progress)
r.insert_exsercise(dfs, id_stream)

# 14. Добавляем слушателей к модулям
r.insert_module(id_stream)
