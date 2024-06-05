import pandas as pd


def read_fios(name):
    """ Считывание ФИО с модуля 1 """
    df = pd.read_excel(name, sheet_name='Модуль 1')
    df = df[['ID пользователя', 'ФИО']]
    return df


def read_os_stream(name):
    '''Считывание файла с ОС по программе '''
    df = pd.read_excel(name, sheet_name='По программе')
    df = df[['ID пользователя',
             'ФИО', 
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько вы удовлетворены организацией обучения?', 
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько вы удовлетворены программой обучения?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько вы удовлетворены взаимодействием с командой и организаторами (эксперт, менеджер, модератор, методист)?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько вы удовлетворены работой менеджера проекта?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько вы удовлетворены работой методиста проекта?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько вы удовлетворены работой тьютора проекта?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Поделитесь своими впечатлениями от взаимодействия с тьютором',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько вы удовлетворены работой экспертов проекта?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Поделитесь своими впечатлениями от взаимодействия с экспертами? ',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько оправдались ваши ожидания?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Что могло бы повысить вашу мотивацию на курсе?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Какие типы заданий/дискуссий вам наиболее интересны?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Что полезного для себя вынесли из программы обучения?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Насколько вы оцениваете свою способность применять результаты обучения на практике?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Какова вероятность, что вы посоветуете обучение в Университете Иннополис?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Какова вероятность, что вы пройдёте обучение в Университете Иннополис по другим программам?',
             'Форма обратной связи по всей программе . Обратная связь по программе:. Здесь вы можете поделиться своими мыслями и впечатлениями в свободной форме',
            ]]
    return df


def read_os_stream_moduls(name):
    '''Считывание файла с ОС по модулям'''
    list_moduls = cheak_sheets(name)
    dfs = {}
    for i in list_moduls:
        df = pd.read_excel(name, sheet_name=i)
        df = df[['ID пользователя',
                 'ФИО',
                 'Насколько вы удовлетворены материалами модуля?',
                 'Насколько вы удовлетворены взаимодействием с командой программы и организаторами?',
                 'На прохождение этого курса вы потратили времени:',
                 'Что могло бы повысить вашу мотивацию на курсе?',
                 'Для выполнения практического задания вы использовали',
                 'Насколько материалы модуля вам понятны?',
                 'Какой полезный опыт вы вынесли из данного модуля?',
                 'Какую информацию вы сразу можете реализовать на практике?',
                 'Что можно усилить в программе обучения?']]
        dfs[i] = df
    return dfs

def read_progress(name):
    '''Открытие файла с прогрессом'''
    xl = pd.ExcelFile(name)
    modules = xl.sheet_names
    dfs = {}
    for module in modules:
        df = pd.read_excel(name, sheet_name=module)
        dfs[module] = df
    return dfs

def read_os_lesson(name):
    list_moduls = cheak_sheets(name)

    dfs = {}
    for i in list_moduls:
        df = pd.read_excel(name, sheet_name=i)
        df = df[[
            'Урок',
            'Упражнение',
            'ФИО',
            'Оценка',
            'Обратная связь'
        ]]
        dfs[i] = df
    return dfs

def read_for_totors(name):
    df = pd.read_excel(name, sheet_name='Модуль 1')
    df = df[['Группа тьютора', 'ФИО']]
    return df

def read_assessment(name):
    df = pd.read_excel(name)

    df = df[[
        'ФИО',
        'Количество попыток',
        'Дата прохождения ассесмента',
        'Наименование компетенции',
        'Планируемый уровень развития компетенции',
        'Итоговый уровень сформированности компетенций',
    ]]
    return df

def read_progress_testing(name, list_moduls):
    dfs = {}
    
    for i in list_moduls:
        df_total = pd.DataFrame()
        df = pd.read_excel(name, sheet_name=i)
        for c in df.columns:
            if c == 'ФИО' or 'Промежуточная аттестация, ' in c:
                df_total[c] = df[c]
        dfs[i] = df_total
    return dfs

def read_progress_for_exrs(name):
    '''Открытие файла с прогрессом для считывания наименований уроков'''
    xl = pd.ExcelFile(name)
    sheets = xl.sheet_names
    dict_exrs = {}
    for sheet in sheets:
        exrs = []
        df = pd.read_excel(name, sheet_name=sheet)
        for i in range(len(df.columns)):
            if (i + 1) %3 == 0:
                exrs.append(df.columns[i])
        exrs = exrs[6:]
        dict_exrs[sheet] = exrs
    return dict_exrs

def cheak_sheets(name):
    xl = pd.ExcelFile(name)
    moduls = xl.sheet_names
    if 'По программе' in moduls:
        moduls.remove('По программе')
    return moduls