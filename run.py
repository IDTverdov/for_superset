
from connect import DataBase


class Run:
    db = None

    def get_connect(self):
        self.db = DataBase()
        self.db.connect()

    def create_programm(self, name, flag):
        '''Создание программы, если такой ещё не было. Если была - достаём id')'''
        self.get_connect()

        if flag:
            self.db.query_insert(f'''INSERT INTO programm (name) VALUES ('{name}');''')
            id_programm = self.db.query('''SELECT id FROM programm ORDER BY 1 DESC LIMIT 1''')[0][0]
        else:
            id_programm = self.db.query(f'''SELECT id FROM programm WHERE name = '{name}' ORDER BY 1 DESC LIMIT 1''')[0][0]
        print(f'Программа "{name}"')
        
        return id_programm
    
    def create_stream(self, number, id_programm):
        '''Создание потока в программе'''
        self.get_connect()
        self.db.query_insert(f'''INSERT INTO stream (number, programm_id) VALUES ({number}, {id_programm})''')
        id_stream = self.db.query('''SELECT id FROM stream ORDER BY 1 DESC LIMIT 1''')[0][0]
        print(f'Поток №{number} создан.')
        return id_stream

    def create_more_modules(self, id_programm, exrs):
        '''Внесение все возможных блоков модулей'''
        self.get_connect()
        modules = [i[0].strip() for i in self.db.query(f'''SELECT name FROM module WHERE programm_id = {id_programm};''')]
        for i in exrs.keys():
            if i.strip() not in modules:
                print(f'Добавляем модуль {i.strip()}')

    
    def create_lesson(self, dfs, id_programm):
        '''Создание уроков в молулях'''
        self.get_connect()

        modules = dfs.keys()

        modules_in_db = self.db.query(f'''SELECT id, name FROM module WHERE progrmm_id = {id_programm};''')
        dict_modules = {}
        for i in modules_in_db:
            dict_modules[i[1]] = i[0]

        for module in modules:
            df = dfs[module]
            list_lessons = df['Урок'].unique()
            id_module = dict_modules[module]
            for i in list_lessons:
                self.db.query_insert(f''' INSERT INTO lesson (name, module_id)
                        VALUES ('{i}', {id_module});''')
            print(f'Добавлены уроки к {module}')
    
    def create_exsercise(self, exsrs, id_programm):
        '''Создание упражнений в уроках'''
        self.get_connect()
        modules = self.db.query(f'''SELECT id, name FROM module WHERE programm_id = {id_programm};''') 

        for idx in range(len(modules)):
            lessons = self.db.query(f'''SELECT id, name FROM lesson WHERE module_id = {modules[idx][0]};''')
            exrs = exsrs[modules[idx][1]]
    
    def insert_user(self, df, id_stream):
        '''Внесение пользователей и добавление на поток'''
        self.get_connect()
        users_in_DB = self.db.query('''SELECT * FROM users;''')

        if users_in_DB:
            users_in_DB = [i[2].strip() for i in users_in_DB]

        count_new_users, ids_shu = 0, []
        for i in df['ID пользователя']:
            if i in users_in_DB:
                id = self.db.query(f'''SELECT id FROM users WHERE id_from_lms = '{i}';''')[0][0]
            else:
                if str(i) != 'nan':
                    fio = (df.loc[df['ID пользователя'] == i]['ФИО'].values[0]).strip()
                    self.db.query_insert(f'''INSERT INTO users (name, id_from_lms) VALUES (
                        '{fio}', '{i}');''')

                    id = self.db.query('''SELECT id FROM users ORDER BY 1 DESC LIMIT 1''')[0][0]
                    count_new_users += 1
            self.db.query_insert(f'''INSERT INTO stream_has_users (users_id, stream_id) VALUES ({id}, {id_stream});''')
            ids_shu.append(self.db.query('''SELECT id FROM stream_has_users ORDER BY 1 DESC LIMIT 1''')[0][0])

        print(f'Добавлено на поток {len(ids_shu)} слушателей. Новых слушателей {count_new_users}.')

    
    def insert_feedback_programm(self, df):
        '''Добавление обратной связи по программе'''
        self.get_connect()
        
        query = '''
            INSERT INTO feedback_programm (
                stream_has_user_id,
                statisfaction_materials,
                statisfaction_comand,
                motivation,
                usefull,
                practical,
                statisfaction_organisation,
                statisfaction_manager,
                statisfaction_metodist,
                statisfaction_tutor,
                feedback_tutor,
                feedback_expert,
                expectations,
                favorite_formate,
                recommendet_UI,
                other_courses_UI,
                thoughts_and_impressions
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s
            );'''
        
        for row in df.itertuples():
            id_shu = self.db.query(f'''SELECT stream_has_users.id FROM stream_has_users
                JOIN users ON stream_has_users.users_id = users.id
                WHERE id_from_lms = '{row[1]}' ORDER BY 1 DESC''')[0][0]
            tuple_for_query = (id_shu,)
            tuple_for_query += tuple([row[i] for i in range(3, 19)])
            self.db.query_insert(query, tuple_for_query)
                    
        print('Обратная связь по итогам программы успешно добавлена')
    
    def insert_module(self, ids_shu, ids_module):
        '''Добавление слушателей на модули'''
        self.get_connect()
        for i in ids_shu:
            for j in ids_module:
                self.db.query_insert(f'''
                    INSERT INTO stream_has_user_has_module (stream_has_user_id, module_id)
                    VALUES ({i}, {j});
                ''')
        print('Слушатели подключены к модулям')

    def insert_feedback_module(self, dfs, id_programm):
        '''Добавление обратной связи по модулям'''
        self.get_connect()

        modules = dfs.keys()
        self.check_moduls(modules, id_programm)

        modules_in_db = self.db.query(f'''SELECT id, name FROM module WHERE progrmm_id = {id_programm};''')
        dict_modules = {}
        for i in modules_in_db:
            dict_modules[i[1]] = i[0]

        query = '''
            INSERT INTO feedback_module (
                stream_has_user_id,
                module_id,
                statisfaction_materials,
                statisfaction_command,
                time_for_pass,
                motivation,
                for_pass_used,
                how_understandable,
                useful,
                practical,
                improve)
            VALUES (
            %s, %s, %s, %s, 
            %s, %s, %s,
            %s, %s, %s, %s);'''
        
        for module in dfs.keys():
            id_module = dict_modules[module]
            for row in dfs[module].itertuples():
                id_shu = self.db.query(
                    f'''SELECT stream_has_users.id FROM stream_has_users
                    JOIN users ON stream_has_users.users_id = users.id
                    WHERE id_from_lms = '{row[1]}' ORDER BY 1 DESC;'''
                )
                  
                tuple_for_query = (id_shu, id_module,)
                tuple_for_query += tuple([row[i] for i in range(3, 12)])
                self.db.query_insert(query, tuple_for_query)
        print(f'Обратная связь по в {module} успешно добавлена')  

    def insert_tutor(self, df, id_stream):
        '''Добавление тьютора, если не было и присоединение к каждому слушателю на потоке'''
        self.get_connect()
        tutors = self.db.query('''SELECT name FROM tutor;''')
        if tutors:
            tutors = [i[0].strip() for i in tutors]
        for row in df.itertuples():
            if row[1] not in tutors:
                self.db.query_insert(f'''INSERT INTO tutor (name) VALUES ('{row[1]}');''' )
                print(f'Добавлен новый тьютор: {row[1]} ')
            id_tutor = self.db.query(f'''SELECT id FROM tutor WHERE name = '{row[1]}';''')[0][0]
            id_shu = self.db.query(
                f'''SELECT stream_has_users.id FROM stream_has_users
                JOIN users ON stream_has_users.users_id = users.id
                WHERE name = '{row[2]}' AND stream_id = {id_stream};''')[0][0]
            self.db.query_insert(f'''INSERT INTO tutor_has_stream_has_user (tutor_id, stream_has_user_id) VALUES ({id_tutor}, {id_shu});''' )
        print('Добавлены тьюторы к слушателям')

    def insert_competence(self, df, id_programm):
        '''Внесение компетенций'''
        self.get_connect()

        list_competence = df['Наименование компетенции'].unique()
        for i in list_competence:
            self.db.query_insert(f'''INSERT INTO competence (name, programm_id) 
                VALUES ('{i}', {id_programm});''')
        print('Добавлены компетенции к программе')

    def insert_assessment(self, df, id_stream):
        '''Внесение результатов ассессмента'''
        self.get_connect()
        query = '''INSERT INTO assessment (competence_id, stream_has_user_id, count_trying, date, want_level, total_level) VALUES
            (%s, %s, %s, %s, %s, %s);'''

        for row in df.itertuples():
            id_shu = self.db.query(
                f'''SELECT stream_has_users.id FROM stream_has_users
                JOIN users ON stream_has_users.users_id = users.id
                WHERE name = '{row[1]}' AND stream_id = {id_stream};''')[0][0]
            id_cometence = self.db.query(f'''
                SELECT id FROM competence WHERE name = '{row[4]}';''')[0][0]

            tuple_for_query = (id_cometence, id_shu, row[2], row[3], row[5], row[6])
            self.db.query_insert(query, tuple_for_query)
        print('Внесена информация по ассессменту')

    def insert_testing(self, dfs, id_programm, id_stream):
        '''Внесение результатов тестирования'''
        self.get_connect()

        modules = dfs.keys()
        self.check_moduls(modules, id_programm)

        query = '''INSERT INTO testing (type_testing, stream_has_user_id, result)
        VALUES (%s, %s, %s)'''
        
        for module in modules:
            if module != 'Бонусный':
                df = dfs[module]
                for row in df.itertuples():
                    id_shu = self.db.query(
                        f'''SELECT stream_has_users.id FROM stream_has_users
                        JOIN users ON stream_has_users.users_id = users.id
                        WHERE name = '{row[1]}' AND stream_id = {id_stream};''')[0][0]
                    type_testing = f'Промежуточный {module}' if module != 'Модуль 5' else 'Итоговый'
                    result = int(row[-1]*100)
                    tuple_for_query = (type_testing, id_shu, result)
                    self.db.query_insert(query, tuple_for_query)
                print(f'Добавлены результаты тестирования в {module}')
        
    def insert_exsercise(self, df, module_id):
        '''Добавление результатов по упражнениям'''
        self.get_connect()

        exrs = self.db.query(f'''SELECT exercise.id, exercise.name FROM exercise 
                             JOIN lesson ON lesson.id = exercise.lesson_id 
                             WHERE module_id = {module_id}''')
        id_prog = self.db.query(f'''SELECT programm_id FROM module WHERE id = {module_id};''')[0][0]
        
        dict_exrs = {}
        for i in exrs:
            dict_exrs[i[1].strip()] = i[0]
        
        query = '''INSERT INTO users_has_exercise (
            exercise_id, score, feedback, stream_has_user_id
        ) VALUES (
            %s, %s, %s, %s
        );'''

        for row in df.itertuples():

            id_shu = self.db.query(f'''SELECT stream_has_users.id FROM stream_has_users 
                JOIN users ON stream_has_users.users_id = users.id 
                JOIN stream ON stream_has_users.stream_id = stream.id
                WHERE programm_id = {id_prog} and users.name = '{row[3]}';''')[0][0]
            tuple_for_query = (dict_exrs[row[2].strip()], row[4] if str(row[4])!= 'nan' else 0, row[5], id_shu)
            self.db.query_insert(query, tuple_for_query)

        module = self.db.query(f'''SELECT name FROM module WHERE id = {module_id}''')[0][0]
        print(f'Добавлены отзывы по упражнениям в {module.strip()}')

    def select_module(self, id_programm):
        '''Если программа создана, выводим список модулей'''
        self.get_connect()
        s = self.db.query(f'''SELECT id, name FROM module WHERE programm_id = {id_programm};''')
        list_module = [i[1] for i in s]
        list_ids_module = [i[0] for i in s]
        return list_module, list_ids_module
    
    def insert_more_module(self, name, id_prog):
        '''Добавление "Доп.материалов"'''
        self.get_connect()

        self.db.query_insert(f'''INSERT INTO module (name, programm_id) 
                VALUES ('{name}', {id_prog});''')
        
        id = self.db.query('''SELECT id FROM module ORDER BY 1 DESC LIMIT 1;''')[0][0]
        print(f'Добавлено {name} к Модулям')
        return id


    def calc_watch(self, df, dict_names, id_stream, module):
        fios = df['ФИО'].unique()
        for fio in fios:
            id_shu = self.db.query(f'''SELECT stream_has_users.id FROM stream_has_users 
                JOIN users ON stream_has_users.users_id = users.id 
                WHERE stream_id = {id_stream} and users.name = '{fio}';''')[0][0]
            df_w = df.loc[df['ФИО'] == fio]

            for i in df_w.columns:
                if i != 'ФИО' and i in dict_names.keys():
                    if df_w[i].values[0] == '0.00':
                        d = self.db.query_insert(f'''UPDATE users_has_exercise SET progress = True 
                                            WHERE stream_has_user_id = {id_shu} AND  exercise_id = {dict_names[i]}''')
        print(f'Просмотры материалов добавлены {module}')

    def check_moduls(self, moduls, id_programm):
        '''Проверка на наличие модулей'''
        self.get_connect()
        moduls_in_db = self.db.query(f'''SELECT name FROM module WHERE programm_id = {id_programm}''')
        if moduls_in_db:
            moduls_in_db = [i[0].strip() for i in moduls_in_db]
        
        for module in moduls:
            if module.strip() not in moduls_in_db:
                self.db.query_insert(f'''INSERT INTO module (name, programm_id) VALUES (
                                     '{module.strip()}', {id_programm}
                ) ''')
                print(f'Добавлен новый модуль {module}')