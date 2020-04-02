CREATE OR REPLACE PROCEDURE MOVEDATA(MOVENUM NUMBER) AS
  V_COUNT   NUMBER;
  V_MOVENUM NUMBER;
  --V_TIMES   NUMBER;
  ERR_NUM  VARCHAR2(100);
  ERR_MSG  VARCHAR2(1000);
  V_ID     VARCHAR2(32);
  V_P_NAME VARCHAR2(100);
BEGIN
  --每次搬运的数量
  V_MOVENUM := MOVENUM;
  --实际搬运的数量
  V_COUNT := V_MOVENUM;
  --搬运次数
  --V_TIMES := 1;
  --搬运分区名称
  V_P_NAME := 'NONE';

  BEGIN
    LOOP
      --生成搬运日志ID
      SELECT SYS_GUID() INTO V_ID FROM DUAL;
      --写入日志
      INSERT INTO A_TEMP
        (ID, TABLE_NAME, PARTITION_NAME)
      VALUES
        (V_ID, 'BS_BASIC_INDEX', V_P_NAME);
      COMMIT;
      --搬运
      INSERT INTO BS_BASIC_INDEX_OLD
        SELECT UNIQUE_ID,
               COL_RES_CODE,
               DOMAIN_CODE,
               LOCAL_ID,
               SELF_NAME,
               BIRTHDAY,
               SEX_CODE,
               ADDRESS,
               CERT_TYPE_CODE,
               CERT_NUM,
               VISCARD_TYPE_CODE,
               VISCARD_NUM,
               BUSINESS_ID,
               BILL_ID,
               SUBMIT_ID,
               SUBMIT_QUEUE,
               TITLE,
               BUSINESS_TYPE_CODE,
               BUSINESS_TYPE_NAME,
               CLASS_CODE,
               CLASS_NAME,
               ACTIVE_TYPE_CODE,
               ACTIVE_TYPE_NAME,
               SERVICE_START_TIME,
               SERVICE_END_TIME,
               SUBMIT_TIME,
               STORAGE_DOMAIN,
               MIMETYPE,
               COLLECT_TYPE,
               SOURCE_STATUS,
               DOC_SIZE,
               SOURCE_ORG_CODE,
               SOURCE_ORG_NAME,
               SOURCE_PERSON_CODE,
               SOURCE_PERSON_NAME,
               SOURCE_CREATE_TIME,
               CREATE_DATE,
               TELEPHONE,
               TO_DATE(CREATE_DATE, 'YYYYMMDDHH24MISS') AS CREATE_DATE_DATE
          FROM (SELECT A.*, ROWNUM RN
                  FROM (SELECT T.*
                          FROM BS_BASIC_INDEX T
                          LEFT JOIN BS_BASIC_INDEX_OLD S
                            ON T.CREATE_DATE IS NOT NULL
                           AND T.UNIQUE_ID = S.UNIQUE_ID
                         WHERE S.UNIQUE_ID IS NULL) A
                 WHERE ROWNUM <= V_MOVENUM)
         WHERE RN > 0;
      --WHERE ROWNUM <= V_MOVENUM * (V_TIMES + 1))
      --WHERE RN > V_MOVENUM * V_TIMES;
    
      --获取实际搬运数量
      V_COUNT := SQL%ROWCOUNT;
      COMMIT;
      --搬运次数+1
      --V_TIMES := V_TIMES + 1;
      --更新搬运日志写入结束时间
      UPDATE A_TEMP T
         SET T.DEAL_END_TIME = SYSDATE, T.DEAL_NUM = V_COUNT
       WHERE ID = V_ID;
      COMMIT;
      --判断搬运是否完成
      EXIT WHEN(V_COUNT <> V_MOVENUM);
    END LOOP;
  
  EXCEPTION
  
    WHEN OTHERS THEN
    
      ERR_NUM := SQLCODE;
    
      ERR_MSG := SUBSTR(SQLERRM, 1, 100);
    
      INSERT INTO A_TEMP
        (TABLE_NAME, PARTITION_NAME, ERROR)
      VALUES
        ('ERRO_INFO', V_P_NAME, ERR_NUM || ERR_MSG);
      COMMIT;
    
  END;
END MOVEDATA;
