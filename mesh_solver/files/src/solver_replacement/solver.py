from solver_replacement.config import *


class meshSolver:
    def __init__(self, db_name):
        self.db_name = db_name
        self.schema_name = targetschema
        self.spark = SparkSession.builder.appName("Solver").getOrCreate()
        self.column_list=['id','createdAt','status','nodes','topology','optimizationType','optimizedTopology','locationProfile','triggers','info','nol','year','month','day','hour','minute','filename'] 
        self.dbutils = DBUtils(self.spark)
    
    def _buildsolverinfoselect(self,sourcetab):
        df=self.spark.sql(f'select * from {sourcetab} limit 1')
        subcluster=[]
        for f in [field for field in df.schema.fields if field.name == 'info']:
            for subfield in f.dataType.fields:
                if subfield.name.startswith('cluster'):
                    print(subfield.name)
                    print(f'size(info.{subfield.name}.ap_list)')
                    subcluster.append(f'size(info.{subfield.name}.ap_list)')       
        strsubcluster=', '.join(subcluster)
        print(strsubcluster)
        solvercluster=[]
        appcluster=[]
        slvtimecluster=[]
        for f in [field for field in df.schema.fields if field.name == 'info']:
            for subfield in f.dataType.fields:
                if subfield.name.startswith('cluster'):
                    print(subfield.name)
                    print(f'size(info.{subfield.name}.ap_list)')
                    solvercluster.append(f'when size(info.{subfield.name}.ap_list) = array_max(array( {strsubcluster})) then info.{subfield.name}.solver_version') 
                    appcluster.append(f'when size(info.{subfield.name}.ap_list) = array_max(array( {strsubcluster})) then info.{subfield.name}.app_version')
                    slvtimecluster.append(f'nvl(try_cast(element_at(info.{subfield.name}.solver_runtime, -1) as double),0)')


        strsolvercluster=' '.join(solvercluster)
        strappcluster=' '.join(appcluster) 
        strslvtimecluster='+'.join(slvtimecluster) 
        strsolverq= ('case  '+ strsolvercluster + ' end solver_version')
        strappq= ( 'case  '+strappcluster+' end  app_version') 
        strslvtimeq= (strslvtimecluster+ ' solver_runtime')  
        return  strsolverq ,strappq ,strslvtimeq
    
    def _buildsolverquery(self,sourcetab,year,month,day,hour,strsolverq,strappq,strslvtimeq):
      
        coresql=(f"""
        with 
        og as (select id as optimizationid
                                ,node_count
                                ,timewindow
                                ,status
                                , firmwareVersion device_firmwareversion,   nodemac
                                ,optimizationtype, partnerid,dfsstate ,Regulatory_domain,triggers
                                ,solver_version,app_version
                                ,solver_runtime
                                ,optimizer_e2e_runtime
                                ,  case when lower(model) like '%plume%' then false else isgateway end isgateway 
                                , case when isgateway  and lower(model) not like '%plume%'  then model end gateway_model
                                , case when  not isgateway    then  model end extender_model
                                ,ogconf.ogconf.channel 
                                ,ogconf.ogconf.channelWidth
                                ,ogconf.ogconf.freqBand 
                                ,ogconf.ogconf.parentId
                                ,ogconf.ogconf.parentVapType
                                ,ogconf.ogconf.region 
                                ,ogconf.ogconf.vaps 
                                ,nol 
                                ,opt_nodemac 
                                ,optconf.optconf.channel  optchannel 
                                ,optconf.optconf.channelWidth optchannelWidth
                                ,optconf.optconf.freqBand optfreqBand
                                ,optconf.optconf.parentId optparentId
                                ,optconf.optconf.parentVapType optparentVapType
                                ,optconf.optconf.region optregion
                                ,optconf.optconf.vaps optvaps
                                ,optwifiConfig
                                ,optimizedtopology 
                            
                from 
                (
                        select id 
                                ,array_size(map_values(nodes))  node_count
                                ,nodes
                                ,optimizationtype
                                ,locationProfile.features.partnerId
                                ,locationProfile.features.dfsMode dfsState
                                ,locationProfile.region as Regulatory_domain 
                                ,triggers
                                ,{strsolverq}
                                ,{strappq}
                                ,{strslvtimeq}
                                 ,cast(info.optimizer_e2e_runtime as double) optimizer_e2e_runtime
                                ,createdAt timewindow
                                ,status
                                ,top.id as nodemac
                                ,top.firmwareversion
                                ,top.model
                                ,top.isgateway
                                ,top.wifiConfig topwifiConfig 
                                ,nol
                                ,opt.opt.id opt_nodemac 
                                ,opt.opt.wifiConfig optwifiConfig 
                                ,optimizedtopology
                        from (select * from {sourcetab} 
                                        where year='{year}' 
                                        and month='{month}' 
                                        and day='{day}' 
                                        and hour='{hour}'   ) solver
                        LATERAL VIEW EXPLODE(topology)  top as top
                        LATERAL VIEW EXPLODE(optimizedtopology)   opt as opt
                        where top.top.id = opt.opt.id
                        )
                         
                     lateral view explode(topwificonfig)  ogconf as ogconf
                     lateral view explode(optwifiConfig )  optconf as optconf
                      where ogconf.freqband = optconf.freqband
                )
        
        ,core as (            
        
        select optimizationid, device_firmwareversion, nodemac
        , isgateway
        ,gateway_model, extender_model,node_count
        ,timewindow
        ,status
        --,cc_status
        ,optimizationtype, partnerid,dfsstate ,Regulatory_domain,triggers
        ,solver_version,app_version
        ,solver_runtime
        ,optimizer_e2e_runtime
        ,freqband
        , array_size(filter( vaps, x -> x = 'home'))  fhvaps
        ,case when len(optparentid )>0 then  freqband end bhtype
        ,optparentid
        ,case when len(optparentid )=0 and len(array_join(optvaps,','))=0 then 1 else 0 end  dormantall
        ,case when optfreqband = '5G' and len(optparentid )=0 and len(array_join(optvaps,','))=0 then 1 else 0 end  dormant5g
        ,case when optfreqband = '5GU' and len(optparentid )=0 and len(array_join(optvaps,','))=0 then 1 else 0 end  dormant5gu
        ,case when optfreqband = '5GL' and len(optparentid )=0 and len(array_join(optvaps,','))=0 then 1 else 0 end  dormant5gl
        ,case when optfreqband = '6G' and len(optparentid )=0 and len(array_join(optvaps,','))=0 then 1 else 0 end  dormant6g
        ,case when optfreqband = '24G' and len(optparentid )=0 and len(array_join(optvaps,','))=0 then 1 else 0 end  dormant24g

        ,case when  freqband = '5G'
                and  (optchannel<> channel
                        or  
                        optchannelwidth<> channelwidth) then 1 else 0 end channelchange5g

        ,case when  freqband = '5GU'
                and  (optchannel<> channel
                        or  
                        optchannelwidth<> channelwidth) then 1 else 0 end channelchange5gu
        ,case when  freqband = '5GL'
                and  (optchannel<>  channel
                        or  
                        optchannelwidth<> channelwidth) then 1 else 0 end channelchange5gl
        ,case when  freqband = '6G'
                and  (optchannel<>  channel
                        or  
                        optchannelwidth<>  channelwidth) then 1 else 0 end channelchange6g
        ,case when  freqband = '2.4G'
                and  (optchannel<>  channel
                        or  
                        optchannelwidth<> channelwidth) then 1 else 0 end channelchange24g
        
        ,case when optchannel<> channel
                        or  
                        optchannelwidth<> channelwidth then 1 else 0 end channelchangeall

        ,nol
        ,case when optchannel in ('52', '56', '60', '64', '100', '104', '108', '112', '116', '120', '124', '128', '132', '136', '140', '144')
        and  EXISTS(nol, x -> x.channel = optchannel and x.state='not_started' ) then 1 else 0 end dfs_bad_rec
        ,case when len(optparentid )>0  and NOT  EXISTS(FLATTEN(TRANSFORM(FILTER(optimizedtopology, x -> x.id = optparentid), x -> x.wifiConfig )),x-> x.channel = optchannel) then 1 else 0 end bhmismatch
        ,case when len(optparentid )>0  and not isgateway then 'Y' end withparent_flag
        ,row_number() over  (partition by optimizationid, nodemac order by optfreqband) nodewin
        ,max(case when len(optparentid )>0  and not isgateway then 'Y' end)   over  (partition by optimizationid, nodemac ) nodewithparent
        ,count(1)   over  (partition by optimizationid, nodemac, freqband) nodebandcount
        from og 
        )

        select optimizationid 
                ,node_count
                ,substr(replace(timewindow,'T',' '),1,13) timewindow
                ,status
                ,optimizationtype, partnerid,dfsstate ,Regulatory_domain
                ,triggers
                , solver_version,app_version
                ,try_cast(solver_runtime as double) solver_runtime
                ,try_cast(optimizer_e2e_runtime as double) optimizer_e2e_runtime
                ,0 topology_change_succeeded
                ,0 topology_change_failed
                , collect_set(device_firmwareversion ) device_firmwareversion
                ,collect_set(gateway_model) gateway_model
                ,collect_set(extender_model) extender_model
                ,collect_set(bhtype) bhtype
                ,max(channelchange5g) channelchange5g
                ,max(channelchange5gl) channelchange5gl
                ,max(channelchange5gu) channelchange5gu
                ,max(channelchange6g) channelchange6g
                ,max(channelchange24g) channelchange24g
                ,max(channelchangeall) channelchangeall
                ,sum(dormantall) dormantall
                ,sum(dormant5g) dormant5g
                ,sum(dormant5gl) dormant5gl
                ,sum(dormant5gu) dormant5gu
                ,sum(dormant6g) dormant6g
                ,sum(dormant24g) dormant24g
                ,sum(case when bhtype = '5G' then 1 else 0 end) bhrecomm5g
                ,sum(case when bhtype = '5GU' then 1 else 0 end) bhrecomm5gu
                ,sum(case when bhtype = '5GL' then 1 else 0 end) bhrecomm5gl
                ,sum(case when bhtype = '6G' then 1 else 0 end) bhrecomm6g
                ,sum(case when bhtype = '2.4G' then 1 else 0 end) bhrecomm24g
                ,sum(case when len(bhtype) >0 then 1 else 0 end) bhrecomm
                ,sum(case when nodewin = 1 and  nodewithparent is null and not isgateway then 1 else 0 end) islanded_nodes
                ,sum(dfs_bad_rec) dfsbadrecomm
                ,sum(bhmismatch) bhmismatch
                ,sum(nodebandcount) bandcount
                ,sum(fhvaps) fhvaps
                ,collect_set(nodemac) nodemac

        from core 
        group by optimizationid 
                ,node_count
                ,substr(replace(timewindow,'T',' '),1,13) 
                ,status
                ,optimizationtype, partnerid,dfsstate ,Regulatory_domain,triggers
                ,solver_version
                ,app_version
                ,try_cast(solver_runtime as double)
                ,try_cast(optimizer_e2e_runtime as double) 
        """) 
        print(coresql)
        return coresql
    
    def _getyearmonthday(self):
 
        df=self.spark.sql(f"""show partitions {sourcetab}""")
        maxdate=df.orderBy(desc(df.columns[0]),desc(df.columns[1]),desc(df.columns[2]),desc(df.columns[3]),desc(df.columns[4]) ).limit(1).first()
        daysdelta=0
        if maxdate[4]=='45' and maxdate[3]=='23':
            print('completed last max date getting next day')
            daysdelta=1
        next_day= (datetime.strptime(f'{maxdate[0]}-{maxdate[1]}-{maxdate[2]}', "%Y-%m-%d")+timedelta(days=daysdelta)).strftime("%Y-%m-%d")
        print(next_day)
        year=next_day.split('-')[0]
        month=next_day.split('-')[1]
        day=next_day.split('-')[2]
        return year,month,day


    def _get_schema(self):

        # 1. Define the schema for the "cluster_n" object
        cluster_schema = StructType([
            StructField("app_version", StringType(), True),
            StructField("solver_mipgap", ArrayType(StringType()), True),
            StructField("solver_status", ArrayType(StringType()), True),
            StructField("ap_list", ArrayType(StringType()), True),
            StructField("solver_version", StringType(), True),
            StructField("solver_objvalue", ArrayType(StringType()), True),
            StructField("solver_runtime", ArrayType(StringType()), True),
            StructField("timestamp", StringType(), True)
        ])
        # 2. Define the info schema
        info_schema = StructType([
            StructField("cluster_0", cluster_schema, True),
            StructField("cluster_1", cluster_schema, True),
            StructField("cluster_2", cluster_schema, True),
            StructField("cluster_3", cluster_schema, True),
            StructField("cluster_4", cluster_schema, True),
            StructField("cluster_5", cluster_schema, True),
            StructField("cluster_6", cluster_schema, True),
            StructField("cluster_7", cluster_schema, True),
            StructField("cluster_8", cluster_schema, True),
            StructField("cluster_9", cluster_schema, True),
            StructField("baseline", StringType(), True) ,
            StructField("optimization_time_since_created", StringType(), True),
            StructField("optimizer_e2e_runtime", StringType(), True)
        ])
        # 3. Define the wifi config schema
        wifi_config_schema = StructType([
            StructField("channel", StringType(), True),
            StructField("channelWidth", StringType(), True),
            StructField("freqBand", StringType(), True),
            StructField("parentId", StringType(), True),
            StructField("vaps", ArrayType(StringType()), True),
            StructField("parentVapType", StringType(), True),
            StructField("region", StringType(), True)
        ])

        # 4. Define the  optimized device 
        device_schema = StructType([
            StructField("id", StringType(), True),
            StructField("model", StringType(), True),
            StructField("nickname", StringType(), True),
            StructField("wifiConfig", ArrayType(wifi_config_schema), True),
            StructField("connectionState", StringType(), True),
            StructField("firmwareVersion", StringType(), True),
            StructField("isGateway", BooleanType(), True)
        ])


        # 5. Define the custom schema
        custom_schema = StructType([
        StructField("chaingains", StringType(), True)
        ,StructField("chanUtilizations", StringType(), True)
        ,StructField("channelGainsOut", StringType(), True)
        ,StructField("cliDlRates", StringType(), True)
        ,StructField("createdAt", StringType(), True)
        ,StructField("edgesFixed", StringType(), True)
        ,StructField("estimatedPhyRate", StringType(), True)
        ,StructField("gatewayNodes", ArrayType(StringType(), True), True)
        ,StructField("id", StringType(), True)  
        ,StructField("info",info_schema, True)
        ,StructField("locationId", StringType(), True)
        ,StructField("locationProfile", StructType([StructField("features", StructType([StructField("configuredDfsMode", StringType(), True)
                                                            ,StructField("dfsMode", StringType(), True)
                                                            ,StructField("excludeChannel13",BooleanType(), True)
                                                            ,StructField("hasSplitSsid",BooleanType(), True)
                                                            ,StructField("hopPenalty", StringType(), True)
                                                            ,StructField("nonDfsClientPresent",BooleanType(), True)
                                                            ,StructField("partnerId", StringType(), True)
                                                            ,StructField("powerManagement", StructType([
                                                                        StructField("mode", StringType(), True)
                                                                        ,StructField("nodes", ArrayType(StringType(), True), True)
                                                                        ,StructField("targetMode", StringType(), True)]), True)
                                                            ,StructField("prefer160Mhz",BooleanType(), True)
                                                            ,StructField("preferMultiple5gHomeVaps",BooleanType(), True)]), True)
                                        ,StructField("profile", StringType(), True)
                                        ,StructField("region", StringType(), True)]), True)

        ,StructField("nodes" , MapType(StringType(), StringType(), True),True)
        ,StructField("nol" , ArrayType(StructType([ 
                    StructField("cacFailureCount", StringType(), True)
                    ,StructField("channel", StringType(), True)
                    ,StructField("freqBand", StringType(), True)
                    ,StructField("nodeId", StringType(), True)
                    ,StructField("radarRateUpdatedAt", StringType(), True)
                    ,StructField("radarsPerHour", StringType(), True)
                    ,StructField("radarsPerHourDecayed", StringType(), True)
                    ,StructField("state", StringType(), True)]),True),True)
        ,StructField("observedPhyrate", StringType(), True)
        ,StructField("optimizationType", StringType(), True)

        ,StructField("optimizedTopology",  ArrayType(device_schema),True)
        ,StructField("model", StringType(), True)
        ,StructField("nickname", StringType(), True)
        ,StructField("wifiConfig",ArrayType(StructType([ StructField("channel", StringType(), True)
                                            ,StructField("channelWidth", StringType(), True)
                                            ,StructField("freqBand", StringType(), True)
                                            ,StructField("parentId", StringType(), True)
                                            ,StructField("parentVapType", StringType(), True)
                                            ,StructField("region", StringType(), True)
                                            ,StructField("vaps",ArrayType(StringType(), True),True)]),True),True)

        ,StructField("origRequest", StringType(), True)
        ,StructField("orphanNodes", StringType(), True)
        ,StructField("planGain", StringType(), True)
        ,StructField("predictions", StringType(), True)
        ,StructField("solverProgress", StringType(), True)
        ,StructField("stats", StringType(), True)
        ,StructField("status", StringType(), True)
        ,StructField("temporaryTopology", StringType(), True)
        ,StructField("topology",ArrayType(StructType([ StructField("connectionState", StringType(), True)
                                ,StructField("firmwareVersion", StringType(), True)
                                ,StructField("id", StringType(), True)
                                ,StructField("isGateway", BooleanType(), True)
                                ,StructField("model", StringType(), True)
                                ,StructField("wifiConfig",ArrayType(StructType([ StructField("channel", StringType(), True)
                                                                            ,StructField("channelWidth", StringType(), True)
                                                                            ,StructField("freqBand", StringType(), True)
                                                                            ,StructField("parentId", StringType(), True)
                                                                            ,StructField("parentVapType", StringType(), True)
                                                                            ,StructField("region", StringType(), True)
                                                                            ,StructField("vaps",ArrayType(StringType(), True),True)]),True),True)]),True),True)

        ,StructField("topologyRelDiff", StringType(), True)
        ,StructField("triggers", ArrayType(StringType(), True), True)
        ])

        return custom_schema


    def sourcedSolverData(self):
        print(sourcetab, backfilldays)
        self.spark.conf.set("fs.s3a.requester-pays.enabled", "true")
        custom_schema=self._get_schema()
        for i in range(int(backfilldays)):
            year,month,day =self._getyearmonthday()
            if self.spark.sql(f"select cast('{year}'||'-'||'{month}'||'-'||'{day}'  as date ) <= current_date"): 
                print('starting ',year,month,day)
                for hr in range(24):
                    hour=str(hr).zfill(2)
                    for min in range(4):
                        minute=str(min*15).zfill(2)
                        print(year,month,day,hour,minute)
                        if self.spark.sql(f"select count(1) from {sourcetab} where year='{year}' and month='{month}' and day='{day}' and hour='{hour}' and minute='{minute}' ").collect()[0][0]==0:
                            path= (f'{sourcepath}/year={year}/month={month}/day={day}/hour={hour}/minute={minute}')
                            try:
                                filecount=len(self.dbutils.fs.ls( path))
                            except:
                                filecount=0
                            if filecount>0:   
                                print(sourcetab) 
                                df2 = self.spark.read.option("mode", "PERMISSIVE").schema(custom_schema).json(path)
                                df2 = df2.withColumn("year",lit(f'{year}')).withColumn("month",lit(f'{month}')).withColumn("day",lit(f'{day}')).withColumn("hour",lit(f'{hour}')).withColumn("minute",lit(f'{minute}')).withColumn("filename",regexp_extract(input_file_name(), ".*/(.*)", 1))
                                df2.select(*self.column_list).write.mode("append").option("mergeSchema", True).partitionBy('year','month','day','hour','minute').saveAsTable(sourcetab)

                        else:
                            print(f'already exists for {sourcetab}' )
            else:
                print(f'All is well and fully loaded on {sourcetab}' )
              
        
    
    def _update_solver_selection(self,targetcoretab,targetselectiontab,timewindows):
        print("Updating dashboard solver replacement filters")
        for timewindow in timewindows:
            dfx=self.spark.sql(f"""select distinct 
                                            gateway_model,
                                            extender_model,
                                            node_count,
                                            optimizationtype,
                                            partnerid,
                                            dfsstate,
                                            Regulatory_domain,
                                            trigger_type,
                                            device_firmware_version,
                                            app_version,
                                            solver_version,
                                            bhtype,
                                            status

                            from (
                            select 
                            explode(array_insert(gateway_model,-1,'All')) gateway_model,
                            explode(array_insert(extender_model,-1,'All')) extender_model,
                            explode(array(cast(node_count as string),'All')) node_count,
                            explode( array(optimizationtype,'All') ) optimizationtype,
                            explode( array(partnerid ,'All') ) partnerid,
                            explode( array(dfsstate,'All') ) dfsstate,
                            explode( array(Regulatory_domain,'All') ) Regulatory_domain,
                            explode(array_insert(triggers,-1,'All')) trigger_type,
                            explode(array_insert(device_firmwareversion,-1,'All')) device_firmware_version,
                            explode( array(app_version,'All') ) app_version,
                            explode( array(solver_version,'All') ) solver_version,
                            explode(array_insert(bhtype,-1,'All')) bhtype,
                            explode( array(status,'All') ) status

                            from (   select gateway_model,
                                            extender_model,
                                            node_count,
                                            optimizationtype,
                                            partnerid,
                                            dfsstate,
                                            Regulatory_domain,
                                            triggers  ,
                                            device_firmwareversion,
                                            app_version,
                                            solver_version,
                                            bhtype,
                                            status 
                                            from {self.db_name}.{self.schema_name}.{targetcoretab}
                                            where timewindow='{timewindow}'
                                            group by 1,2,3,4,5,6,7,8,9,10,11,12,13)
                            )
            
                    """).createOrReplaceTempView('newselection') 
            self.spark.sql(f"""merge into {self.db_name}.{self.schema_name}.{targetselectiontab} tgt
                           using newselection src
                        on (tgt.gateway_model=src.gateway_model and tgt.extender_model=src.extender_model and tgt.node_count=src.node_count and tgt.optimizationtype=src.optimizationtype and tgt.partnerid=src.partnerid and tgt.dfsstate=src.dfsstate and tgt.Regulatory_domain=src.Regulatory_domain and tgt.trigger_type=src.trigger_type and tgt.device_firmware_version=src.device_firmware_version and tgt.app_version=src.app_version and tgt.solver_version=src.solver_version and tgt.bhtype=src.bhtype and tgt.status=src.status)
                        when not matched then 
                        insert (gateway_model,
                                extender_model,
                                node_count,
                                optimizationtype,
                                partnerid,
                                dfsstate,
                                Regulatory_domain,
                                trigger_type,
                                device_firmware_version,
                                app_version,
                                solver_version,
                                bhtype,
                                status)
                        values(src.gateway_model,
                                src.extender_model,
                                src.node_count,   
                                src.optimizationtype,
                                src.partnerid,
                                src.dfsstate,
                                src.Regulatory_domain,
                                src.trigger_type,
                                src.device_firmware_version,
                                src.app_version,
                                src.solver_version,
                                src.bhtype,
                                src.status)
                        
                       """)    
        ########dfx.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable(self.db_name+'.'+self.schema_name+'.'+targetselectiontab)

 
    def _buildcoreSolverDatabyhour(self,sourcetab,targetcoretab,year,month,day,hour,strsolverq,strappq,strslvtimeq):
        solverquery=self._buildsolverquery(sourcetab,year,month,day,hour,strsolverq,strappq,strslvtimeq)
        print(solverquery)
        dfsolvercore=self.spark.sql(solverquery)
        self.spark.sql(f""" delete from {self.db_name}.{self.schema_name}.{targetcoretab} where timewindow = concat('{year}','-','{month}','-','{day}',' ','{hour}')
                           """)
        dfsolvercore.write.mode('append').option('mergeSchema', 'true').partitionBy('timewindow').saveAsTable(self.db_name+'.'+self.schema_name+'.'+targetcoretab)


    def buildcoreSolverData(self):
        print(sourcetab)
        strsolverq,strappq,strslvtimeq=self._buildsolverinfoselect(sourcetab)
        timewindows=[]
        for row in self.spark.sql(f'''select concat(year,'-',month,'-',day,' ',hour) timewindow from {sourcetab} 
                        group by 1  having count(distinct minute) = 4
                        minus
                        select distinct timewindow from   {self.db_name}.{self.schema_name}.{targetcoretab}
                        ''' ).collect():
            print(row)
            ### check existence in target table
            timewindows.append(row.timewindow)
            print(f'processing {row.timewindow}')
            self._buildcoreSolverDatabyhour(sourcetab,targetcoretab,row.timewindow[:4],row.timewindow[5:7],row.timewindow[8:10],row.timewindow[11:13], strsolverq,strappq,strslvtimeq)
        if len(timewindows)>0  :
            print(f'Rebuild selection data')
            self._update_solver_selection(targetcoretab,targetselectiontab,timewindows)


    def updateSolverChannelChangeStatus(self):
        self.spark.sql(f""" 
                merge into {self.db_name}.{self.schema_name}.{targetcoretab} tgt
                using (select distinct dt,optimizationid, source_type
                                                        ,case when topology_change_status='succeeded' then 'succeeded' else 'failed' end cc_status 
                                                        from {self.db_name}.{self.schema_name}.{cctab} where source_type='MESH' and dt >= date_sub(current_date(),{backfilldays}))  src
                on (tgt.optimizationid = src.optimizationid
                and instr(tgt.timewindow,src.dt) > 0 
                )
                when matched then update 
                set tgt.topology_change_succeeded = case when cc_status='succeeded' then 1 else 0 end ,
                tgt.topology_change_failed =  case when cc_status<>'succeeded' then 1 else 0 end 
                """)
