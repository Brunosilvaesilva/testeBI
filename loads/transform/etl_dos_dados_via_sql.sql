-- 1 Valores carregados do arquivo csv para as tabelas ETLs.

--drop table ETL_CNAE
	create table ETL_CNAE (
					cd_cnpj				varchar(max) null ,
					nu_ordem			varchar(max) null ,
					cd_ramo_atividade	varchar(max) null,
					de_ramo_atividade	varchar(max) null );
	
	BULK INSERT ETL_CNAE
	FROM 'C:\CNAE.csv'
	WITH (
	FIELDTERMINATOR = ';',
	rowterminator = '0x0a');
----	
	CREATE TABLE ETL_empresas(
						[cd_cnpj] [varchar](max) NULL,
						[fl_matriz] [varchar](max) NULL,
						[dt_abertura] [varchar](max) NULL,
						[nm_razao_social] [varchar](max) NULL,
						[cd_natureza_juridica] [varchar](max) NULL,
						[de_natureza_juridica] [varchar](max) NULL,
						[nm_logradouro] [varchar](max) NULL,
						[nu_logradouro] [varchar](max) NULL,
						[cd_cep] [varchar](max) NULL,
						[nm_bairro] [varchar](max) NULL,
						[nm_municipio] [varchar](max) NULL,
						[sg_uf] [varchar](max) NULL,
						[de_situacao] [varchar](max) NULL,
						[de_classif_natureza_juridica] [varchar](max) NULL);

	BULK INSERT ETL_empresas
	FROM 'C:\Users\bruno\OneDrive\Trabalho\Empresas\Neoway\Git\loads\extract\empresas.csv'
	WITH (
	FIELDTERMINATOR = ';',
	rowterminator = '0x0a');
----

	CREATE TABLE ETL_empresasCalc(
		[cd_cnpj] [varchar](max) NULL,
		[vl_latitude] [nvarchar](max) NULL,
		[vl_longitude] [varchar](max) NULL,
		[vl_total_veiculos_antt] [varchar](max) NULL,
		[vl_total_veiculos_leves] [varchar](max) NULL,
		[vl_total_veiculos_pesados] [varchar](max) NULL,
		[fl_optante_simples] [varchar](max) NULL,
		[qt_filial] [varchar](max) NULL,
		[fl_optante_simei] [varchar](max) NULL,
		[de_saude_tributaria] [varchar](max) NULL,
		[de_nivel_atividade] [varchar](max) NULL
	)
	
	BULK INSERT ETL_empresasCalc
		FROM 'C:\Users\bruno\OneDrive\Trabalho\Empresas\Neoway\Git\loads\extract\empresasCalc.csv'
		WITH (
		FIELDTERMINATOR = ';',
		rowterminator = '0x0a');
---
	
	CREATE TABLE ETL_setores_ramos_atividade(
		[Ramo_de_Atividade] [nvarchar](50) NULL,
		[Divisoes] [nvarchar](150) NOT NULL,
		[CNAE_divisao_dois_digitos] [nvarchar](50) NOT NULL,
		[F4] [nvarchar](1) NULL,
		[F5] [nvarchar](1) NULL,
		[F6] [nvarchar](1) NULL,
		[F7] [nvarchar](1) NULL
	)
	
	BULK INSERT ETL_setores_ramos_atividade
		FROM 'C:\Users\bruno\OneDrive\Trabalho\Empresas\Neoway\Git\loads\extract\Setores e Ramos de Atividade.csv'
		WITH (
		FIELDTERMINATOR = ';',
		rowterminator = '0x0a');

--- Carrega tabela com com o cadastro do CNAE por empresas. 

	--drop table d_cnae_empresas 
	create table d_cnae_empresas (
				cd_cnpj					varchar(max) null, 
				nu_ordem				varchar(max) null, 
				cd_cnae_dois_digitos	varchar(max) null,
				cd_ramo_atividade		varchar(max) null,
				de_ramo_atividade		varchar(max) null
				)

	INSERT INTO d_cnae_empresas  (cd_cnpj, nu_ordem, cd_cnae_dois_digitos, cd_ramo_atividade, de_ramo_atividade)
	select 
		* 
	from (
		SELECT 
			A.cd_cnpj, 
			a.value				as nu_ordem, 
			left(b.value, 2)	as cd_cnae_dois_digitos, 
			b.value				as cd_ramo_atividade, 
			c.value				as de_ramo_atividade 
		FROM (
			SELECT  
				ROW_NUMBER ( ) OVER ( PARTITION BY cd_cnpj ORDER BY cd_cnpj )-1 AS RWN,
				cd_cnpj, 
				value
			FROM ETL_CNAE 
			CROSS APPLY STRING_SPLIT(replace(nu_ordem, '~@~', ';'),';')) A
			JOIN (
				SELECT 
					ROW_NUMBER ( ) OVER ( PARTITION BY cd_cnpj ORDER BY cd_cnpj )-1 AS RWN, 
					cd_cnpj, 
					value
				FROM ETL_CNAE  
				CROSS APPLY STRING_SPLIT(replace(cd_ramo_atividade, '~@~', ';'),';'))B ON A.cd_cnpj = B.cd_cnpj AND A.RWN = B.RWN
			JOIN (
				SELECT 
					ROW_NUMBER ( ) OVER ( PARTITION BY cd_cnpj ORDER BY cd_cnpj )-1 AS RWN ,
					cd_cnpj, 
					value
				FROM ETL_CNAE 
				CROSS APPLY STRING_SPLIT(replace(de_ramo_atividade, '~@~', ';'),';'))C ON A.cd_cnpj = C.cd_cnpj AND A.RWN = C.RWN
			) d
	order by cd_cnpj 

--- Carrega tabela com o cadastro das empresas. 

	create table d_Empresas (
	   [cd_cnpj]							varchar(max)
      ,[fl_matriz]							varchar(max)
      ,[dt_abertura]						date
      ,[nm_razao_social]					varchar(max)
      ,[cd_natureza_juridica]				varchar(max)
      ,[de_natureza_juridica]				varchar(max)
      ,[nm_logradouro]						varchar(max)
      ,[nu_logradouro]						varchar(max)
      ,[cd_cep]								varchar(max)
      ,[nm_bairro]							varchar(max)
      ,[nm_municipio]						varchar(max)
      ,[sg_uf]								varchar(max)
      ,[de_situacao]						varchar(max)
      ,[de_classif_natureza_juridica]		varchar(max) 
	  )


	insert into d_Empresas 
	SELECT 
		   [cd_cnpj]					
		  ,[fl_matriz]					
		  ,convert(date, dt_abertura, 110) as dt_abertura			
		  ,[nm_razao_social]			
		  ,[cd_natureza_juridica]		
		  ,[de_natureza_juridica]		
		  ,[nm_logradouro]				
		  ,[nu_logradouro]				
		  ,[cd_cep]						
		  ,[nm_bairro]					
		  ,[nm_municipio]				
		  ,[sg_uf]						
		  ,[de_situacao]				
		  ,[de_classif_natureza_juridica]
	FROM [dbo].[ETL_empresas]

-- Carrega tabela empresaCalc

	create table d_empresaCalc (
		  [cd_cnpj]						varchar(20)
	      ,[vl_latitude]				varchar(max)
	      ,[vl_longitude]				varchar(max)
	      ,[vl_total_veiculos_antt]		varchar(max)
	      ,[vl_total_veiculos_leves]	varchar(max)
	      ,[vl_total_veiculos_pesados]	varchar(max)
	      ,[fl_optante_simples]			varchar(max)
	      ,[qt_filial]					varchar(max)
	      ,[fl_optante_simei]			varchar(max)
	      ,[de_saude_tributaria]		varchar(max)
	      ,[de_nivel_atividade]			varchar(max)
	)
	insert into d_empresaCalc
	SELECT [cd_cnpj]
	      ,[vl_latitude] 
		  ,[vl_longitude]
		  ,[vl_total_veiculos_antt]
		  ,[vl_total_veiculos_leves]
		  ,[vl_total_veiculos_pesados]
		  ,[fl_optante_simples]
		  ,[qt_filial]	
	      ,[fl_optante_simei]
	      ,[de_saude_tributaria]
	      ,[de_nivel_atividade]
	  FROM [dbo].[ETL_empresasCalc]

-- Carrega Setores e Ramos de Atividade.
	create table d_setores_ramos_atividade (
		[Ramo_de_Atividade]				varchar(max)
      ,[Divisoes]						varchar(max)
      ,[cd_CNAE_dois_digitos]		varchar(max)
)

	insert into d_setores_ramos_atividade
	SELECT 
		  [Ramo_de_Atividade]
	      ,[Divisoes]
	      ,[CNAE_divisao_dois_digitos] as [cd_CNAE_dois_digitos]
	  FROM [dbo].[ETL_setores_ramos_atividade]


  ---Relacionamento entre dimensões (Utilizado no primeiro modelo)

	select 
		e.cd_cnpj, c.cd_cnae_dois_digitos
	from d_Empresas e
	join d_cnae_empresas c on c.cd_cnpj = e.cd_cnpj
	join d_empresaCalc   ec on ec.cd_cnpj = e.cd_cnpj
	join d_setores_ramos_atividade sra on sra.cd_cnae_dois_digitos = c.cd_cnae_dois_digitos

--Views(Não cheguei a utlizar essas consultas no Power BI, acabei usando o dax para algumas classificações, mas usei de teste)

  -- Empresas por faixa de veículo.

  select distinct c.cd_cnpj, e.nm_razao_social, 
	sum(cast(ec.vl_total_veiculos_leves as int)) + sum(cast(ec.vl_total_veiculos_pesados as int)) as vl_total_veiculos,
	case
		when sum(cast(ec.vl_total_veiculos_leves as int)) + sum(cast(ec.vl_total_veiculos_pesados as int))			   <=  5 then 1
		when sum(cast(ec.vl_total_veiculos_leves as int)) + sum(cast(ec.vl_total_veiculos_pesados as int)) between  6 and 10 then 2
		when sum(cast(ec.vl_total_veiculos_leves as int)) + sum(cast(ec.vl_total_veiculos_pesados as int)) between 11 and 15 then 3
		when sum(cast(ec.vl_total_veiculos_leves as int)) + sum(cast(ec.vl_total_veiculos_pesados as int)) between 16 and 30 then 4 
		when sum(cast(ec.vl_total_veiculos_leves as int)) + sum(cast(ec.vl_total_veiculos_pesados as int)) between 31 and 50 then 5
		when sum(cast(ec.vl_total_veiculos_leves as int)) + sum(cast(ec.vl_total_veiculos_pesados as int))              > 50 then 6
	else 0 end as range_qt_veiculos
  from d_Empresas e
	join d_cnae_empresas c on c.cd_cnpj = e.cd_cnpj
	join d_empresaCalc   ec on ec.cd_cnpj = e.cd_cnpj
	join d_setores_ramos_atividade sra on sra.cd_cnae_dois_digitos = c.cd_cnae_dois_digitos
  where ec.vl_total_veiculos_leves is not null and ec.vl_total_veiculos_pesados is not null
  group by c.cd_cnpj, e.nm_razao_social,ec.vl_total_veiculos_leves, ec.vl_total_veiculos_pesados
  order by range_qt_veiculos

  -- Empresas por nível e atividade 

  select 
	c.cd_cnpj, 
	e.nm_razao_social, 
	ec.de_nivel_atividade, 
	sra.Ramo_de_Atividade, 
	sra.divisoes, 
	sra.cd_CNAE_dois_digitos, 
	ec.vl_latitude,
	ec.vl_longitude,
	concat(e.sg_uf, ',', e.nm_municipio, ',',e.nm_bairro, ',', e.nm_logradouro, ',', e.cd_cep) as endereco
  from d_Empresas e
	join d_cnae_empresas c on c.cd_cnpj = e.cd_cnpj
	join d_empresaCalc   ec on ec.cd_cnpj = e.cd_cnpj
	join d_setores_ramos_atividade sra on sra.cd_cnae_dois_digitos = c.cd_cnae_dois_digitos


