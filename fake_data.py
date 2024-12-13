import random
import time
import uuid
from cassandra.cluster import Cluster
from faker import Faker
import threading

# Khởi tạo Faker
fake = Faker()

# Kết nối Cassandra
cluster = Cluster(['127.0.0.1'])  # Địa chỉ Cassandra của bạn
session = cluster.connect('my_keyspace')  # Thay 'your_keyspace' bằng tên keyspace của bạn

select_query = """
    SELECT create_time
    FROM mytable
    WHERE create_time IS NOT NULL AND campaign_id IS NOT NULL 
    LIMIT 10  # Chỉ lấy một số lượng nhỏ để tránh lấy quá nhiều dữ liệu
"""
# Câu lệnh UPDATE để chỉ cập nhật 4 cột cụ thể dựa trên create_time và campaign_id
update_query = """
    UPDATE mytable
    SET 
        job_id=%s,
        group_id=%s
    WHERE create_time = %s 
"""

# Các giá trị có thể có cho custom_track
# Hàm để cập nhật dữ liệu ngẫu nhiên mỗi 20 giây
def update_random_data():
    while True:
        # Chọn một create_time ngẫu nhiên (hoặc từ dữ liệu hiện tại)
      # Hoặc lấy một create_time có sẵn từ bảng

        job_id = random.choice([34, 44])  # Lấy một số ngẫu nhiên từ danh sách
        group_id = random.choice([20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30])  # Lấy một group_id ngẫu nhiên

        # Cập nhật dữ liệu vào Cassandra
        session.execute(update_query, (
            job_id, group_id
        ))

        print(f"Updated record with job_id={job_id}, group_id={group_id}")

        time.sleep(20)  # Đợi 20 giây trước khi cập nhật hàng tiếp theo
# Hàm để dừng chương trình
def stop_program():
    print("Exiting program...")
    global running
    running = False

# Khởi tạo trạng thái dừng chương trình
running = True

# Khởi động luồng để cập nhật dữ liệu mỗi 20 giây
update_thread = threading.Thread(target=update_random_data)
update_thread.daemon = True
update_thread.start()

# Lệnh thoát khi cần
try:
    while running:
        pass  # Giữ chương trình chạy mãi cho đến khi người dùng dừng
except KeyboardInterrupt:
    stop_program()  # Dừng chương trình khi nhận tín hiệu thoát
