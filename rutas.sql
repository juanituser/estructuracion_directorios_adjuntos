
-- Fijar esquemas
set search_path to 	
	campo, -- Esquema del modelo de captura en campo
	public;
	
drop table if exists rutas;

CREATE TABLE IF NOT EXISTS rutas AS
(with adjuntos_terreno as (
    select 
        substring(cp.numero_predial, 1, 2) as dpto,
        substring(cp.numero_predial, 3, 3) as mpio,
        substring(cp.numero_predial, 6, 2) as zona,
        cp.numero_predial as predio,
        cat.archivo as url_documento,
        '_AC' as sufijo
    from cca_predio cp
    left join cca_terreno ct on ct.predio=cp.t_id
    left join cca_adjuntoterrenovalor cat on cat.cca_terreno_adjunto = ct.t_id
    where cat.archivo like 'http%'-- Solo los subidos al repo
),
adjuntos_interesados as (
    select 
        substring(cp.numero_predial, 1, 2) as dpto,
        substring(cp.numero_predial, 3, 3) as mpio,
        substring(cp.numero_predial, 6, 2) as zona,
        cp.numero_predial as predio,
        cai.archivo as url_documento,
        '_DI' as sufijo,
        ROW_NUMBER () OVER (
			PARTITION BY cp.numero_predial
    		ORDER BY cai.t_seq 
    	) 
    from cca_predio cp
    left join cca_derecho cd on cd.predio = cp.t_id
    left join cca_interesado ci on ci.t_id = cd.interesado
    left join cca_adjuntointeresadovalor cai on cai.cca_interesado_adjunto = ci.t_id
    where cai.archivo like 'http%'-- Solo los subidos al repo
),
adjuntos_fuente as (
    select 
        substring(cp.numero_predial, 1, 2) as dpto,
        substring(cp.numero_predial, 3, 3) as mpio,
        substring(cp.numero_predial, 6, 2) as zona,
        cp.numero_predial as predio,
        caf.archivo as url_documento,
        case 
		WHEN cf.tipo in (select t_id FROM cca_fuenteadministrativatipo where ilicode like '%Acto_Administrativo%')
    	THEN '_AD'
    	WHEN cf.tipo in (select t_id FROM cca_fuenteadministrativatipo where ilicode like '%Escritura_Publica%')
    	THEN '_EP'
    	WHEN cf.tipo in (select t_id FROM cca_fuenteadministrativatipo where ilicode like '%Sentencia_Judicial%')
    	THEN '_SJ'
    	WHEN cf.tipo in (select t_id FROM cca_fuenteadministrativatipo where ilicode like '%Documento_Privado%')
    	THEN '_DP'
    	else concat('_',(select ilicode FROM cca_fuenteadministrativatipo where cf.tipo=t_id))
	end as sufijo,
	ROW_NUMBER () OVER (
	PARTITION BY cp.numero_predial
    ORDER BY
      caf.t_seq -- cambiar en interesado
    ) 
    from cca_predio cp
    left join cca_derecho cd on cd.predio = cp.t_id
    left join cca_fuenteadministrativa cf on cf.derecho = cd.t_id
    inner join cca_adjuntofuenteadministrativavalor caf on caf.cca_fuenteadminstrtiva_adjunto = cf.t_id
    where caf.archivo like 'http%'-- Solo los subidos al repo
),
adjuntos_unidades as (
    select 
        substring(cp.numero_predial, 1, 2) as dpto,
        substring(cp.numero_predial, 3, 3) as mpio,
        substring(cp.numero_predial, 6, 2) as zona,
        cp.numero_predial as predio,
        cau.archivo as url_documento,
        case
        	when cau.tipo_elemento in (select t_id from cca_adjuntoelementotipo where ilicode like 'Fachada')
        	then '_FAC'
        	when cau.tipo_elemento in (select t_id from cca_adjuntoelementotipo where ilicode like 'Acabados')
        	then '_ACA'
        	when cau.tipo_elemento in (select t_id from cca_adjuntoelementotipo where ilicode like 'Estructura')
        	then '_EST'
        	when cau.tipo_elemento in (select t_id from cca_adjuntoelementotipo where ilicode like 'Banios')
        	then '_BAN'
        	when cau.tipo_elemento in (select t_id from cca_adjuntoelementotipo where ilicode like 'Cocina')
        	then '_COC'
        	when cau.tipo_elemento in (select t_id from cca_adjuntoelementotipo where ilicode like 'Cerchas')
        	then '_CER'
        	when cau.tipo_elemento in (select t_id from cca_adjuntoelementotipo where ilicode like 'Unidad_No_Convencional')
        	then '_NC'
        	when cau.tipo_elemento in (select t_id from cca_adjuntoelementotipo where ilicode like 'Lote')
        	then '_LOTE'
        	else '_OTRO'
        end as sufijo,	
	cca.identificador as identificador
    from cca_predio cp
    left join cca_unidadconstruccion cu on cu.predio=cp.t_id
    left join cca_caracteristicasunidadconstruccion cca on cca.t_id=cu.caracteristicasunidadconstruccion  
    left join cca_adjuntounidadconstruccionvalor cau on cau.cca_unidadconstruccion_adjunto = cu.t_id
    where cau.archivo like 'http%'-- Solo los subidos al repo
)
------------------------------------------
------------ ADJUNTOS TERRENO ------------
------------------------------------------
select
	'adjuntos_terreno' as tabla_adjuntos,
    concat(
        at.dpto, '/', 
        at.mpio, '/', 
        at.zona,
        case
            when at.zona = '01' then '_rur/'
            when at.zona = '02' then '_urb/'
            else 'zona_no_valida/'
        end,
        '01_especif/01_',
        substring(at.predio, 6, 25),        
        '/01_colin'
    ) as ruta_base,
    concat(at.predio, at.sufijo) as nombre,
    at.url_documento as url_archivo,
    at.predio,
     at.sufijo
from adjuntos_terreno at
where at.url_documento is not null
------------------------------------------
---------- ADJUNTOS INTERESADOS ----------
------------------------------------------
union
select
	'adjuntos_interesado' as tabla_adjuntos,
    concat(
        ai.dpto, '/', 
        ai.mpio, '/', 
        ai.zona,
        case
            when ai.zona = '01' then '_rur/'
            when ai.zona = '02' then '_urb/'
            else 'zona_no_valida/'
        end,
        '01_especif/01_',
        substring(ai.predio, 6, 25),        
        '/02_doc_sop'
    ) as ruta_base,
    case 
   		when ai.row_number > 1 
    	then concat(ai.predio, ai.sufijo, ai.row_number)
    	else concat(ai.predio, ai.sufijo)
   	end as nombre,
    ai.url_documento as url_archivo,
    ai.predio,
    ai.sufijo
from adjuntos_interesados ai
where ai.url_documento is not null
------------------------------------------
----- ADJUNTOS FUENTE ADMINISTRATIVA -----
------------------------------------------
union
select
	'adjuntos_fuenteAdm' as tabla_adjuntos,
    concat(
        af.dpto, '/', 
        af.mpio, '/', 
        af.zona,
        case
            when af.zona = '01' then '_rur/'
            when af.zona = '02' then '_urb/'
            else 'zona_no_valida/'
        end,
        '01_especif/01_',
        substring(af.predio, 6, 25),        
        '/02_doc_sop'
    ) as ruta_base,
   case 
   	when af.row_number > 1 
    then concat(af.predio, af.sufijo, af.row_number)
    else concat(af.predio, af.sufijo)
   end as nombre,
    af.url_documento as url_archivo,
    af.predio,
    af.sufijo
from adjuntos_fuente af
where af.url_documento is not null
-------------------------------------------
---- ADJUNTOS UNIDADES DE CONSTRUCCIÓN ----
-------------------------------------------
union
select
	'adjuntos unidades' as tabla_adjuntos,
    concat(
        au.dpto, '/', 
        au.mpio, '/', 
        au.zona,
        case
            when au.zona = '01' then '_rur/'
            when au.zona = '02' then '_urb/'
            else 'zona_no_valida/'
        end,
        '01_especif/01_',
        substring(au.predio, 6, 25),        
        '/03_img'
    ) as ruta_base,
    concat(au.predio, '_',au.identificador , au.sufijo) as nombre,
    au.url_documento as url_archivo,
    au.predio,
     au.sufijo
from adjuntos_unidades au
where au.url_documento is not null);

COPY rutas TO 'C:/Users/PC/Documents/CEICOL/insumos_consolidados/leiva_zona_2_4/adjuntos/rutas.csv' WITH (FORMAT CSV, HEADER);
