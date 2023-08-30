import pymysql
import yaml
from chandaotask.log_util import logger
from chandaotask.log_utill import logger1
import time

# 记录需求ID的集合
logged_story_ids = set()


def log1_message(message):
    if message not in logged_story_ids:
        print(message)
        logger1.info(message)  # 记录日志的具体代码
        logged_story_ids.add(message)


logged_messages = set()


def log_message(message):
    if message not in logged_messages:
        print(message)
        logger.info(message)  # 记录日志的具体代码
        logged_messages.add(message)


def create_test_tasks():
    # 连接数据库
    # 读取data.yaml文件
    with open('data.yaml', 'r', encoding='utf-8') as file:
        data = yaml.safe_load(file)

    # 获取数据库连接配置
    db_config = data['db_config']

    # 建立数据库连接
    db = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        port=db_config['port'],
        cursorclass=pymysql.cursors.DictCursor
    )

    def get_story_info(cursor, limit=200):
        query = "SELECT id, product, title, type, pri, status, fromVersion, openedBy, openedDate, assignedTo, assignedDate, version, reviewedDate FROM zentao.zt_story ORDER BY id DESC LIMIT %s"
        cursor.execute(query, (limit,))
        stories = cursor.fetchall()
        return stories

    # 创建任务单
    try:
        with db.cursor() as cursor:
            # 从zentao.zt_story表中获取所需信息
            stories = get_story_info(cursor)

            for story in stories:
                # 查找相同title的spec信息
                query_spec = "SELECT spec FROM zentao.zt_storyspec WHERE title = %s"
                cursor.execute(query_spec, story['title'])
                spec = cursor.fetchone()
                spec_value = spec['spec'] if spec else ""

                # 根据产品id获取指派人信息
                with open('data.yaml', 'r', encoding='utf-8') as file:
                    data = yaml.safe_load(file)
                product_key = f'product{story["product"]}'
                if product_key in data:
                    test_assignee = data[product_key]['test_assignee']
                    client_assignee = data[product_key]['client_assignee']
                    server_assignee = data[product_key]['server_assignee']
                else:
                    test_assignee = 'wugengzhou'
                    client_assignee = 'wugengzhou'
                    server_assignee = 'wugengzhou'

                # 检查需求下是否存在任务单
                query_task = "SELECT COUNT(*) as count FROM zentao.zt_task WHERE story = %s"
                cursor.execute(query_task, story['id'])  # 自定义内容 + 标题
                task_count = cursor.fetchone()['count']

                if task_count == 0:
                    # 不存在任务，创建新的任务
                    # 判断条件2: 检查关联关系及类型是否满足
                    check_query = "SELECT COUNT(*) AS count FROM zentao.zt_projectstory ps JOIN zentao.zt_project p ON ps.project = p.id WHERE ps.story = %s AND p.type IN ('project', 'sprint')"
                    cursor.execute(check_query, story['id'])
                    check_result = cursor.fetchone()

                    if check_result['count'] == 0:
                        print(f"不满足project和execution同时存在，不创建任务：{story['title']}")

                        continue

                    # 获取project字段的值
                    project_query = "SELECT p.id FROM zentao.zt_story s JOIN zentao.zt_projectstory ps ON s.id = ps.story JOIN zentao.zt_project p ON p.id = ps.project WHERE s.id = %s AND p.type = 'project'"
                    cursor.execute(project_query, story['id'])
                    project_result = cursor.fetchone()

                    if project_result is None:
                        print(f"没有project，不创建任务：{story['title']}")

                        continue

                    project = project_result['id']

                    # 获取execution字段的值
                    execution_query = "SELECT p.id FROM zentao.zt_story s JOIN zentao.zt_projectstory ps ON s.id = ps.story JOIN zentao.zt_project p ON p.id = ps.project WHERE s.id = %s AND p.type = 'sprint'"
                    cursor.execute(execution_query, story['id'])
                    execution_result = cursor.fetchone()

                    if execution_result is None:
                        print(f"没有execution，不创建任务：{story['title']}")

                        continue
                    execution = execution_result['id']

                    if "【资源】" in story['title']:

                        task_data = [
                            {
                                "title": f"【测试】{story['title']}",
                                "assignedTo": test_assignee,
                                "type": "test"
                            },
                            {
                                "title": f"【客户端】{story['title']}",
                                "assignedTo": client_assignee,
                                "type": "devel"
                            }
                        ]

                        for data in task_data:
                            insert_task_query = "INSERT INTO zentao.zt_task (story, project, name, execution, type, pri, status, storyVersion, openedBy, openedDate, assignedTo, assignedDate, version, `desc`,design,designVersion,feedback,mode,estimate,consumed,`left`,color) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, 0, 0, 0, 0, 0)"
                            cursor.execute(insert_task_query, (
                                story['id'],
                                project,
                                data['title'],
                                execution,
                                data['type'],
                                story['pri'],
                                "wait",
                                story['fromVersion'],
                                story['openedBy'],
                                story['openedDate'],
                                data['assignedTo'],
                                story['assignedDate'],
                                story['version'],
                                spec_value
                            ))
                            db.commit()

                            print(f"只创建测试任务单和客户端任务单：{data['title']}")
                            log_message(f"只创建测试任务单和客户端任务单：{story['id']}，标题：{data['title']}")
                            log1_message(f"{story['id']}")
                    else:
                        # 创建任务单的标题、指派人和类型列表
                        task_data = [
                            {
                                "title": f"【测试】{story['title']}",
                                "assignedTo": test_assignee,
                                "type": "test"
                            },
                            {
                                "title": f"【服务端】{story['title']}",
                                "assignedTo": server_assignee,
                                "type": "devel"
                            },
                            {
                                "title": f"【客户端】{story['title']}",
                                "assignedTo": client_assignee,
                                "type": "devel"
                            }
                        ]

                        for data in task_data:
                            insert_task_query = "INSERT INTO zentao.zt_task (story, project, name, execution, type, pri, status, storyVersion, openedBy, openedDate, assignedTo, assignedDate, version, `desc`,design,designVersion,feedback,mode,estimate,consumed,`left`,color) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, 0, 0, 0, 0, 0)"
                            cursor.execute(insert_task_query, (
                                story['id'],
                                project,
                                data['title'],
                                execution,
                                data['type'],
                                story['pri'],
                                "wait",
                                story['fromVersion'],
                                story['openedBy'],
                                story['openedDate'],
                                data['assignedTo'],
                                story['assignedDate'],
                                story['version'],
                                spec_value
                            ))
                            db.commit()

                            print(f"创建所有任务单成功：{data['title']}")
                            log_message(f"创建所有任务单成功：{story['id']}，标题：{data['title']}")
                            log1_message(f"{story['id']}")
                else:
                    print(f"需求{story['id']}已存在任务")
    except Exception as e:
        print(f"创建需求{story['id']}对应任务单发生错误：{e}")
        logger.error(f"创建需求{story['id']}对应任务单发生错误：{e}")
        db.rollback()
    finally:
        pass

    # 创建验收任务单
    try:
        with db.cursor() as cursor:
            # 从zentao.zt_story表中获取所需信息
            query = "SELECT id, product, title, type, pri, status, fromVersion, openedBy, openedDate, assignedTo, assignedDate, version,reviewedDate FROM zentao.zt_story ORDER BY id DESC LIMIT 200"
            cursor.execute(query)
            stories = cursor.fetchall()

            for story in stories:
                # 查找相同title的spec信息
                query_spec = "SELECT spec FROM zentao.zt_storyspec WHERE title = %s"
                cursor.execute(query_spec, story['title'])
                spec = cursor.fetchone()

                # 检查相同标题的开发单是否全部已完成
                query_dev = "SELECT COUNT(*) AS count FROM zentao.zt_task WHERE story = %s AND type = 'devel'"
                cursor.execute(query_dev, story['id'])
                dev_count = cursor.fetchone()['count']

                if dev_count > 0:
                    query_dev_done = "SELECT COUNT(*) AS count FROM zentao.zt_task WHERE story = %s AND type = 'devel' AND status = 'done'"
                    cursor.execute(query_dev_done, story['id'])
                    dev_done_count = cursor.fetchone()['count']

                    if dev_count == dev_done_count:
                        # 创建任务单前先检查是否存在需求是否有关联的验收任务
                        # cleaned_title = re.sub('\【.*?\】', '', story['title'])
                        # cleaned_title = re.sub(':.*$', '', cleaned_title)
                        query_task = "SELECT COUNT(*) AS count FROM zentao.zt_task WHERE story = %s and type= 'design'"
                        cursor.execute(query_task, story['id'])  # 自定义内容 + 标题
                        task_count = cursor.fetchone()['count']

                        if task_count == 0:
                            # 获取相同标题的测试任务单的 project 字段和 execution 字段
                            query_test_task = "SELECT project, execution FROM zentao.zt_task WHERE story = %s AND type = 'test'"
                            cursor.execute(query_test_task, story['id'])
                            test_task = cursor.fetchone()

                            if test_task:
                                # 任务单不存在，创建新的任务单
                                insert_task_query = "INSERT INTO zentao.zt_task (story,design,project, name, execution, type, pri, status, storyVersion, openedBy, openedDate, assignedTo, assignedDate, version, `desc`,designVersion,feedback,mode,estimate,consumed,`left`,color) VALUES (%s,0,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,0,0,0,0,0,0,0)"
                                cursor.execute(insert_task_query, (
                                    story['id'],
                                    test_task['project'],  # 使用相同标题测试任务单的 project 字段
                                    f"【验收】{story['title']}",  # 自定义内容 + 标题
                                    test_task['execution'],  # 使用相同标题测试任务单的 execution 字段
                                    "design",
                                    story['pri'],
                                    "wait",
                                    story['fromVersion'],
                                    story['openedBy'],
                                    story['openedDate'],
                                    story['openedBy'],
                                    story['assignedDate'],
                                    story['version'],
                                    spec['spec'] if spec else ""
                                ))
                                db.commit()

                                print(f"创建策划任务单成功：{story['title']}")
                                log_message(f"已创建验收任务单需求ID：{story['id']}，标题：{story['title']}")

                            else:
                                print(f"没有找到相同标题的测试任务单，无法创建策划任务单：{story['title']}")

                        else:
                            print(f"策划任务单已存在，跳过：{story['title']}")

                    else:
                        print(f"相同需求的开发任务单未全部完成，跳过创建策划任务单：{story['title']}")

                else:
                    print(f"相同需求没有开发任务单，跳过创建策划任务单：{story['title']}")

    except Exception as e:
        print(f"创建策划单{story['title']}发生错误：{e}")
        logger.error(f"创建策划单{story['title']}发生错误：{e}")
        db.rollback()

    finally:
        pass

    # 3.验收单完成后转给对应的测试单指派人
    try:
        with db.cursor() as cursor:
            # 查询所有标题包含【验收】、【策划】、【客户端】、【服务端】的任务单
            query_task = "SELECT id, name, assignedTo, story FROM zentao.zt_task WHERE name LIKE '【验收】%' or name LIKE '【策划】%' or name LIKE '【客户端】%' or name LIKE '【服务端】%' ORDER BY id DESC LIMIT 500"
            cursor.execute(query_task)
            tasks = cursor.fetchall()

            # 遍历任务单列表
            for task in tasks:
                # 检查任务单的状态是否为已完成
                query_status = "SELECT status FROM zentao.zt_task WHERE id = %s"
                cursor.execute(query_status, task['id'])
                status = cursor.fetchone()['status']

                if status == 'done':
                    # 获取测试任务单的指派人
                    query_test_assignedTo = "SELECT assignedTo FROM zentao.zt_task WHERE story = %s AND type = 'test'"
                    cursor.execute(query_test_assignedTo, task['story'])
                    test_assignedTo = cursor.fetchone()

                    if test_assignedTo and task['assignedTo'] != test_assignedTo['assignedTo']:
                        # 更新验收任务单的指派人为测试任务单的指派人
                        update_assignedTo = "UPDATE zentao.zt_task SET assignedTo = %s WHERE id = %s"
                        cursor.execute(update_assignedTo, (test_assignedTo['assignedTo'], task['id']))
                        db.commit()
                        print(f"更新任务单指派人成功：{task['name']}")
                        log_message(f"任务{task['id']}:更新任务单指派人成功")

                    else:
                        print(f"验收任务单指派人和测试任务单指派人相同或者测试任务单不存在，无需更新：{task['name']}")
            print("最后200条已完成的任务单指派人更新完成")
    except Exception as e:
        print(f"更新任务单指派人发生错误：{e}")
        logger.error(f"更新任务单指派人发生错误：{e}")
        db.rollback()

    finally:
        pass
    # 4.禅道需求取消后，关闭需求单下面的任务
    try:
        with db.cursor() as cursor:
            # 查询需求单状态是否为 'closed'
            query_story = "SELECT id, status FROM zentao.zt_story WHERE status = 'closed'"
            cursor.execute(query_story)
            stories = cursor.fetchall()

            for story in stories:
                # 查询关联的任务单
                query_tasks = "SELECT id, status FROM zentao.zt_task WHERE story = %s"
                cursor.execute(query_tasks, story['id'])
                tasks = cursor.fetchall()

                all_tasks_closed = True  # 假设关联任务单全部已关闭

                for task in tasks:
                    if task['status'] != 'closed':
                        all_tasks_closed = False
                        break

                if all_tasks_closed:
                    print(f"需求单id为{story['id']}的需求下的所有任务单已关闭，无需继续操作")
                    continue  # 跳过关闭操作

                # 更新相关任务单状态为 'closed'
                update_task_status = "UPDATE zentao.zt_task SET status = 'closed' WHERE story = %s"
                cursor.execute(update_task_status, story['id'])

                db.commit()
                print(f"成功关闭需求单下的关联任务：{story['id']}")
                log_message(f"成功关闭需求单{story['id']}下的关联任务")

        print("所有关联任务单状态已更新为 'closed'")

    except Exception as e:
        print(f"禅道需求取消后，关闭需求单下面的任务发生错误：{e}")
        logger.error(f"禅道需求取消后，关闭需求单下面的任务发生错误：{e}")
        db.rollback()

    finally:
        pass

    # 5.禅道需求改关联版本后，需求对应的任务也改关联版本
    try:
        with db.cursor() as cursor:
            # 查询需求单和关联任务单的信息
            query = """
                    SELECT s.id, s.title AS story_title, ts.id AS task_id, ts.project, ts.execution
                    FROM zentao.zt_story s 
                    JOIN zentao.zt_task ts ON s.id = ts.story
                """
            cursor.execute(query)
            results = cursor.fetchall()

            for result in results:
                story_id = result['id']  # 将需求的 ID 赋值给变量 story_id

                # 获取需求的项目编号
                project_query = """
                        SELECT p.id 
                        FROM zentao.zt_story s 
                        JOIN zentao.zt_projectstory ps ON s.id = ps.story
                        JOIN zentao.zt_project p ON ps.project = p.id 
                        WHERE s.id = %s AND p.type = 'project'
                    """
                cursor.execute(project_query, story_id)  # 使用 story_id
                project_result = cursor.fetchone()

                if project_result is None:
                    print(f"需求 {story_id} 无法找到对应的项目，请检查数据")
                    continue

                project_id = project_result['id']

                # 获取需求的产品编号
                product_query = """
                        SELECT p.id 
                        FROM zentao.zt_story s 
                        JOIN zentao.zt_projectstory ps ON s.id = ps.story
                        JOIN zentao.zt_project p ON ps.project = p.id 
                        WHERE s.id = %s AND p.type IN ('sprint', 'kanban')
                    """
                cursor.execute(product_query, story_id)  # 使用 story_id
                product_result = cursor.fetchone()

                if product_result is None:
                    print(f"需求 {story_id} 无法找到对应的迭代任务，请检查数据")
                    continue

                product_id = product_result['id']

                # 对比任务单的项目编号和产品编号是否一致，如果一致则不更新
                if result['project'] != project_id or result['execution'] != product_id:
                    # 更新任务单的项目编号和产品编号
                    update_query = "UPDATE zentao.zt_task SET project = %s, execution = %s WHERE id = %s"
                    cursor.execute(update_query, (project_id, product_id, result['task_id']))
                    db.commit()

                    print(f"任务单 {result['task_id']} 的项目编号和产品编号已更新")
                    log_message(f"任务 {result['task_id']} 的项目编号和产品编号已更新")


                else:
                    print(f"任务单 {result['task_id']} 的项目编号和产品编号与需求相同，无需更新")

        print("更新任务单关联需求版本完成")


    except Exception as e:
        print(f"任务更新关联版本发生错误：{e}")
        logger.error(f"任务更新关联版本发生错误：{e}")
        db.rollback()

    finally:
        db.close()


if __name__ == "__main__":
    while True:
        create_test_tasks()
        time.sleep(60)  # 每60秒运行一次
