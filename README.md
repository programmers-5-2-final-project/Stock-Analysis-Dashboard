   # Stock Analysis Dashboard

<p align='center'>
<a href='https://docs.google.com/document/d/11YNi1o6gkr928iFAXmCZS3aNRUwyBIomxcg5uuQX6uw/edit?usp=sharing'>보고서</a> | 
<a href='https://prgrms.notion.site/5-2-6c895c3daf3943878ef08ef60ad6ebc9?pvs=4'>팀 노션</a> | 
<a href='https://github.com/programmers-5-2-final-project/Stock-Analysis-Dashboard'>데이터 파이프라인 코드</a> | 
<a href='https://github.com/programmers-5-2-final-project/Front'>대시보드 코드</a> | 
</p>

<br></br>
## 프로젝트 개요
### 1. 내용  
한국거래소, 나스닥, S&P, 원자재 가격 데이터를 가져와서 변환하고 저장하는 데이터 파이프라인을 구축하고 분석 대시보드, 웹서비스 구현하였습니다.

### 2. 결과물
#### 대시보드
![web_dashboard1](https://github.com/team6-group2/movie-data-backend/assets/65884076/65c9acbb-d341-4a83-9d1b-b7f5475e2436)
![web_dashboard1](https://github.com/team6-group2/movie-data-backend/assets/65884076/65c9acbb-d341-4a83-9d1b-b7f5475e2436)
![web_dashboard1](https://github.com/team6-group2/movie-data-backend/assets/65884076/65c9acbb-d341-4a83-9d1b-b7f5475e2436)
#### DAGs

### 3. 프로젝트 기간 : 2023.08 ~ 2023.09
​
### 4. 아키텍처

![web_dashboard1](https://github.com/team6-group2/movie-data-backend/assets/65884076/65c9acbb-d341-4a83-9d1b-b7f5475e2436)

### 5. 데이터플로우

### 6. 기술 스택

- 언어: Python3
- 데이터 파이프라인: Airflow, AWS Redshift, AWS RDS, AWS S3
- 프론트엔드: Superset, chart.js, css
- 백엔드: Flask, PostgreSQL
- 머신러닝: Keras, sklearn
- 인프라: Docker, Git, Git Action   
<br></br>

### 7. 참여자 정보 및 각 역할
|이름|역할|
|:---:|:---:|
|김종욱(kjw9684k)|AWS Infra, Superset, Airflow DAG 작성 (ETL), flask 백엔드, Git Actions CI/CD 구성|
|국승원(Aiant5615)|API 조사, Airflow DAG 작성(ETL, ELT), Superset, ML|
|박다혜(hyedall)|Airflow DAG 작성 (ETL), flask 백엔드, css, chart.js 프론트엔드 웹 구현|
|조민동(sanso62)|Airflow DAG 작성(ETL, ELT), Superset, flask 백엔드, chart.js, Docker|  


### 8. 프로젝트 설치 및 실행방법 (현재 aws를 내려서 정상 작동하지 않음.)
​
​#### 데이터 파이프라인, Superset
1. 깃 레포지토리 클론
   ```sh
   $ git clone https://github.com/programmers-5-2-final-project/Stock-Analysis-Dashboard.git
   ```
2. 데이터 웨어하우스, AWS 정보가 있는 .env 생성
3. 실행
   ```sh
   $ sh run.sh
   ```
#### Flask

1. 깃 레포지토리 클론
   ```sh
   $ git clone https://github.com/programmers-5-2-final-project/Front.git
   ```
2. .flaskenv > flask 폴더 안에 구성 POSTGRES_USER POSTGRES_PASSWORD POSTGRES_HOST POSTGRES_PORT
3. 실행
   ```sh
   $ python3 flask/app.py
   ```
4. http://127.0.0.1:5000/main/home 접속
