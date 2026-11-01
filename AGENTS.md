# Hướng dẫn làm việc — dbt-starrocks

## Ngữ cảnh Engineering

Fork adapter dbt StarRocks, phát hành dưới tên dbt-starrocks-droppii.

Đọc [ngữ cảnh repository](docs/repository-context.md) để chọn đúng module, toolchain, kiểm tra và đường phát hành. Đọc hướng dẫn thư mục con khi đi vào phạm vi tương ứng.

Chuẩn dùng chung: [AI Context](https://github.com/droppii/engineering-knowledge/tree/docs/repository-context-pilot/handbook/ai-context/README.md) và [Standards](https://github.com/droppii/engineering-knowledge/tree/docs/repository-context-pilot/handbook/standards/standards.md). Ghi revision đã dùng và kết quả kiểm chứng trong PR; cập nhật ngữ cảnh cùng mã.

Giữ tương thích dbt-core/StarRocks, kiểu số/decimal, render relation, incremental và cancellation. Kiểm tra unit riêng với functional test cần StarRocks; release package là bước riêng với chạy pytest.

## Profile thực thi

Đọc [.engineering/README.md](.engineering/README.md) để chọn profile theo module và nhu cầu kiểm chứng. Dùng checkout/worktree do công cụ agent quản lý theo công việc; phân biệt kiểm nguồn, kiểm mã và kiểm thử hành vi.
