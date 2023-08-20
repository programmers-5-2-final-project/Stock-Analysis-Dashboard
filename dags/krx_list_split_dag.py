'''
✨전체적인 흐름

1.  raw_data.krx_list -> analytics.kospi_list, analytics.kosdaq_list, analytics.konex_list
    postgres partition으로 분리, 의존성도 있고 메모리 자원도 아낌, 즉 데이터를 효율적으로 관리.
    적재하는데 너무 많은 시간이 걸림.


✨Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.

✨Domain 특징
1. http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd?baseName=krx.mdc.i18n.component&key=B128.bld
2. cols_map = {'ISU_SRT_CD':'Code', 'ISU_ABBRV':'Name', 
                'TDD_CLSPRC':'Close', 'SECT_TP_NM': 'Dept', 'FLUC_TP_CD':'ChangeCode', 
                'CMPPREVDD_PRC':'Changes', 'FLUC_RT':'ChagesRatio', 'ACC_TRDVOL':'Volume', 
                'ACC_TRDVAL':'Amount', 'TDD_OPNPRC':'Open', 'TDD_HGPRC':'High', 'TDD_LWPRC':'Low',
                'MKTCAP':'Marcap', 'LIST_SHRS':'Stocks', 'MKT_NM':'Market', 'MKT_ID': 'MarketId' }
'''