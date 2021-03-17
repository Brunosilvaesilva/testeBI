-- 1 Valores carregados do arquivo csv para as tabelas ETLs.

--- Carrega tabela com com o cadastro do CNAE por empresas. 

	create table d_cnae_empresas (
				[cd_cnpj]				[varchar](max) NULL,
				[cd_cnae_dois_digitos]	[varchar](max) NULL,
				[nu_ordem_1]			[varchar](max) NULL,
				[nu_ordem_2]			[varchar](max) NULL,
				[nu_ordem_3]			[varchar](max) NULL,
				[nu_ordem_4]			[varchar](max) NULL,
				[nu_ordem_5]			[varchar](max) NULL,
				[cd_ramo_atividade_1]	[varchar](max) NOT NULL,
				[cd_ramo_atividade_2]	[varchar](max) NULL,
				[cd_ramo_atividade_3]	[varchar](max) NULL,
				[cd_ramo_atividade_4]	[varchar](max) NULL,
				[cd_ramo_atividade_5]	[varchar](max) NULL,
				[de_ramo_atividade_1]	[varchar](max) NOT NULL,
				[de_ramo_atividade_2]	[varchar](max) NULL,
				[de_ramo_atividade_3]	[varchar](max) NULL,
				[de_ramo_atividade_4]	[varchar](max) NULL,
				[de_ramo_atividade_5]	[varchar](max) NULL
				)
	
	insert into d_cnae_empresas 
	select
		left(concat(cd_cnpj, '00'),14)				as cd_cnpj
		,cast(left(cd_ramo_atividade_1, 2) as int)	as cd_cnae_dois_digitos			
		,[nu_ordem_1]		
		,[nu_ordem_2]		
		,[nu_ordem_3]		
		,[nu_ordem_4]		
		,[nu_ordem_5]		
		,[cd_ramo_atividade_1]
		,[cd_ramo_atividade_2]
		,[cd_ramo_atividade_3]
		,[cd_ramo_atividade_4]
		,[cd_ramo_atividade_5]
		,[de_ramo_atividade_1]
		,[de_ramo_atividade_2]
		,[de_ramo_atividade_3]
		,[de_ramo_atividade_4]
		,[de_ramo_atividade_5]	
	from cnae_empresas_pivot

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
	      ,[cd_CNAE_dois_digitos]
	  FROM [dbo].[ETL_setores_ramos_atividade]


  ---Relacionamento entre dimensões

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


