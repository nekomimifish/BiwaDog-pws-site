BiwaDog-pws-site/
├── index.html ← 主网页文件
├── data.json ← 本地气象数据（示例）
└── README.md ← 项目说明文件（本文件）


---

## 🌡️ data.json 示例
```json
{
  "station": {
    "id": "PWS-001",
    "name": "自宅屋顶",
    "location": {"lat": 35.0, "lon": 135.0}
  },
  "latest": {
    "timestamp": "2025-10-20T10:31:00+09:00",
    "temperature_c": 22.8,
    "humidity_pct": 63,
    "pressure_hpa": 1012.6,
    "wind_ms": 2.4
  },
  "history": [
    {"t": "2025-10-19T11:00:00+09:00", "temp": 24.1, "hum": 58, "hpa": 1011.8, "wind": 2.0},
    {"t": "2025-10-19T12:00:00+09:00", "temp": 25.2, "hum": 56, "hpa": 1011.5, "wind": 2.2}
  ]
}

⚙️ 本地运行

1️⃣ 在项目目录中启动本地服务器：

python -m http.server 8080


2️⃣ 打开浏览器访问：

http://localhost:8080


3️⃣ 在页面顶部选择「本地 JSON」并点击“加载数据”。

☁️ 接入远程数据（可选）

如果你有传感器设备（如 ESP32 / Raspberry Pi）：

设备可定时上传气象数据到服务器（例如 Flask、Node.js、Serverless）。

前端可将数据源切换为「远程 API」，实时获取最新气象信息。

🧰 技术栈

HTML5 + CSS3

JavaScript (ES6)

Chart.js

可扩展至 Flask / Netlify Functions / Vercel API

💡 作者

nekomimifish
个人研究项目：琵琶湖流域观测与环境数据可视化
📍 Ritsumeikan University, 2025 Autumn
