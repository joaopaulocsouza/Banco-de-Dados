DROP SEQUENCE survey_seq;
DROP TRIGGER trg_survey;

DROP TABLE platform;
DROP TABLE lang;
DROP TABLE dev_type;
DROP TABLE learn_code;
DROP TABLE database_;
DROP TABLE employment;
DROP TABLE survey;

CREATE SEQUENCE survey_seq
START WITH 1
INCREMENT BY 1;


CREATE TABLE survey(
    id INT,
    country VARCHAR(80),
    ed_level VARCHAR(90),
    years_code NUMBER,
    age VARCHAR(30),
    CONSTRAINT result_pk PRIMARY KEY (id)
);

CREATE TABLE platform (
    response_id INT,
    platform VARCHAR(100),
    FOREIGN KEY (response_id) REFERENCES survey(id)
);

CREATE TABLE lang (
    response_id INT,
    lang VARCHAR(100),
    FOREIGN KEY (response_id) REFERENCES survey(id)
);

CREATE TABLE dev_type (
    response_id INT,
    dev_type VARCHAR(150),
    FOREIGN KEY (response_id) REFERENCES survey(id)
);

CREATE TABLE learn_code (
    response_id INT,
    learn_code VARCHAR(150),
    FOREIGN KEY (response_id) REFERENCES survey(id)
);

CREATE TABLE database_ (
    response_id INT,
    database_ VARCHAR(150),
    FOREIGN KEY (response_id) REFERENCES survey(id)
);

CREATE TABLE employment(
    response_id INT,
    employment VARCHAR(200),
    FOREIGN KEY (response_id) REFERENCES survey(id)
);


CREATE OR REPLACE TRIGGER trg_survey
BEFORE INSERT ON survey
FOR EACH ROW
BEGIN
    SELECT survey_seq.NEXTVAL
    INTO :NEW.id
    FROM DUAL;
END;
/
-- Funcao para transformar string de anos em number
CREATE OR REPLACE FUNCTION parse_years_code(years_code_in VARCHAR) RETURN NUMBER IS
BEGIN
    IF REGEXP_LIKE(years_code_in, '^[0-9]+$') THEN
        RETURN TO_NUMBER(years_code_in);
    END IF;
    CASE 
        WHEN years_code_in = 'More than 50 years' THEN
            RETURN 51;
        WHEN years_code_in = 'Less than 1 year' THEN
            RETURN 0;
        WHEN years_code_in = 'NA' THEN
            RETURN 0; 
        ELSE    
            RETURN 0;
    END CASE;
END;
/

-- Procedimento para inserir os dados nas tabelas de 2022
-- Inicialmente insere dados que não precisam ser tratados
-- Depois ajusta dados que estão foras do padrão

CREATE OR REPLACE PROCEDURE insert_survey_data_2022 AS
    aux VARCHAR(400);
    v_string VARCHAR(150);
    v_seq NUMBER;
    v_years NUMBER;
BEGIN
    FOR c IN (SELECT 
                country, 
                age, 
                edlevel, 
                yearscode, 
                employment, 
                learncode, 
                devtype, 
                languagehaveworkedwith, 
                platformhaveworkedwith, 
                databasehaveworkedwith
              FROM 
                stackoverflow.so_2022) LOOP

        SELECT parse_years_code(c.yearscode) INTO v_years FROM DUAL;
         
        INSERT INTO survey(country, age, ed_level, years_code)
        VALUES (c.country, c.age, c.edlevel, v_years);
        
        SELECT survey_seq.CURRVAL INTO v_seq FROM DUAL;

         -- insere na tabela de plataforma 
         -- percorre string separando nos ; e insere se for diferente de null
         aux := c.platformhaveworkedwith;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO platform(platform, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;

         -- insere na tabela de linguagem
         aux := c.languagehaveworkedwith;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO lang(lang, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;
         
          -- insere na tabela de employment
         aux := c.employment;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO employment(employment, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;

         -- insere na tabela de dev_type
         aux := c.devtype;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO dev_type(dev_type, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;

         -- insere na tabela de learn_code
         aux := c.learncode;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO learn_code(learn_code, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;
         
         -- insere na tabela de database
         aux := c.databasehaveworkedwith;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO database_(database_, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;

    END LOOP;
END insert_survey_data_2022;
/

-- Procedimento para inserir os dados nas tabelas de 2021
-- Inicialmente insere dados que não precisam ser tratados
-- Depois ajusta dados que estão foras do padrão

CREATE OR REPLACE PROCEDURE insert_survey_data_2021 AS
    aux VARCHAR(400);
    v_string VARCHAR(150);
    v_seq NUMBER;
    v_years NUMBER;
BEGIN
    FOR c IN (SELECT 
                country, 
                age, 
                edlevel, 
                yearscode, 
                employment, 
                learncode, 
                devtype, 
                languagehaveworkedwith, 
                platformhaveworkedwith, 
                databasehaveworkedwith
              FROM 
                stackoverflow.so_2021) LOOP

        SELECT parse_years_code(c.yearscode) INTO v_years FROM DUAL;
         
        INSERT INTO survey(country, age, ed_level, years_code)
        VALUES (c.country, c.age, c.edlevel, v_years);
        
        SELECT survey_seq.CURRVAL INTO v_seq FROM DUAL;

         -- insere na tabela de plataforma
         aux := c.platformhaveworkedwith;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO platform(platform, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;

         -- insere na tabela de linguagem
         aux := c.languagehaveworkedwith;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO lang(lang, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;

         -- insere na tabela de dev_type
         aux := c.devtype;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO dev_type(dev_type, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;

         -- insere na tabela de learn_code
         aux := c.learncode;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO learn_code(learn_code, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;
         
          -- insere na tabela de employment
         aux := c.employment;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO employment(employment, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;
         
         -- insere na tabela de database
         aux := c.databasehaveworkedwith;
         FOR i IN 1..LENGTH(aux) - LENGTH(REPLACE(aux, ';', '')) + 1 LOOP
             v_string := SUBSTR(aux, 1, INSTR(aux, ';') - 1);
             aux := SUBSTR(aux, INSTR(aux, ';') + 1);
            
             IF v_string IS NOT NULL THEN
                 INSERT INTO database_(database_, response_id)
                 VALUES (v_string, v_seq);
             END IF;
         END LOOP;

    END LOOP;
END insert_survey_data_2021;
/

BEGIN
   insert_survey_data_2022;
   COMMIT;
    insert_survey_data_2021;
    COMMIT;
END;
/


CREATE INDEX idx_country ON survey(country);
CREATE INDEX idx_platform_id ON platform (response_id);
DROP INDEX idx_platform_id;
DROP INDEX idx_country;

-- Devs por plataforma por pais
-- custo 1488 sem index o custo é o mesmo
SELECT s.country AS pais, p.platform AS plataforma, COUNT(*) AS devs_por_plataforma
FROM survey s
INNER JOIN platform p ON s.id = p.response_id
GROUP BY s.country, p.platform
ORDER BY s.country, devs_por_plataforma DESC;

CREATE INDEX idx_db_id ON database_ (response_id);

-- Devs por banco
SELECT database_, COUNT(*) AS devs_por_banco
FROM survey s
INNER JOIN database_ db ON s.id = db.response_id
GROUP BY database_
ORDER BY devs_por_banco DESC;


SELECT
    learn_code,
    ed_level,
    COUNT(*) AS n_de_respostas,
    AVG(years_code) AS media_anos_programando
FROM survey s
JOIN learn_code lc ON lc.response_id = s.id
JOIN lang l ON l.response_id = s.id
GROUP BY ed_level, learn_code;


CREATE INDEX idx_emp_id ON employment (response_id);
DROP INDEX idx_emp_id;

-- Media de anos desenvolvendo de brasileiros que aprenderam online
-- Agrupados por tipo de db e profissao
-- custo esitmado com index em country e na tabela employment 1996
-- sem o index continua o msm custo
WITH online_learners AS (
  SELECT s.id, s.country, s.age
  FROM survey s
  INNER JOIN learn_code lc ON lc.response_id = s.id
  WHERE lc.learn_code LIKE '%Online%' OR lc.learn_code LIKE '%online%'
)
SELECT e.employment AS profissao, db.database_, AVG(s.years_code) AS media_anos_programando
FROM online_learners o
INNER JOIN survey s ON s.id = o.id
INNER JOIN database_ db ON db.response_id = s.id
INNER JOIN employment e ON e.response_id = s.id
WHERE o.country LIKE '%Brazil%'
GROUP BY o.country, e.employment, db.database_;


-- Linguagens e tipos de desenvolvedores mais comuns
-- custo estimado 7186
SELECT dt.dev_type AS tipo_dev, l.lang AS linguagem, COUNT(*) AS total
FROM survey s
INNER JOIN platform p ON p.response_id = s.id
INNER JOIN lang l ON l.response_id = s.id
INNER JOIN dev_type dt ON dt.response_id = s.id
GROUP BY dt.dev_type, l.lang
ORDER BY total DESC;

CREATE INDEX idx_lang_id ON lang (response_id);
DROP INDEX idx_lang_id
--plataformas mais usadas por pessoas empregadas em período integral, agrupadas por lingaugem de programacao.
--custo estimado 337 com index, sem index 748
WITH platform_count AS (
    SELECT s.id, p.platform, COUNT(*) AS total
    FROM survey s
    INNER JOIN platform p ON p.response_id = s.id
    GROUP BY s.id, p.platform
),
platform_rank AS (
    SELECT pc.id, pc.platform, pc.total, RANK() OVER (PARTITION BY pc.id ORDER BY pc.total DESC) AS rank
    FROM platform_count pc
),
platform_rank_filter AS (
    SELECT pr.id, pr.platform, pr.total
    FROM platform_rank pr
    WHERE pr.rank = 1
)
SELECT l.lang AS linguagem, prf.platform AS plataforma, COUNT(*) AS total
FROM platform_rank_filter prf
INNER JOIN survey s ON s.id = prf.id
INNER JOIN lang l ON l.response_id = s.id
INNER JOIN employment e ON e.response_id = s.id
WHERE e.employment LIKE '%full-time%'
GROUP BY prf.platform, l.lang
ORDER BY total DESC;