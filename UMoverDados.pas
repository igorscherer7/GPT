unit UMoverDados;
interface
uses
  System.SysUtils, System.Classes, FireDAC.Stan.Intf, FireDAC.Comp.BatchMove,
  FireDAC.Stan.Option, FireDAC.Stan.Error, FireDAC.UI.Intf, FireDAC.Phys.Intf,
  FireDAC.Stan.Def, FireDAC.Stan.Pool, FireDAC.Stan.Async, FireDAC.Phys,
  FireDAC.Phys.PG, FireDAC.Phys.PGDef, FireDAC.VCLUI.Wait, Data.DB, IOUtils,
  FireDAC.Comp.Client, FireDAC.Comp.BatchMove.SQL,
  FireDAC.Comp.BatchMove.DataSet, Vcl.Dialogs, FireDAC.Phys.Oracle,
  FireDAC.Phys.OracleDef, FireDAC.Stan.Param, FireDAC.DatS, FireDAC.DApt.Intf,
  FireDAC.DApt, FireDAC.Comp.DataSet, FireDAC.Comp.BatchMove.JSON;
type
  TDtmMoverDados = class(TDataModule)
    FDBatchMove1: TFDBatchMove;
    FDBatchMoveSQLReader1: TFDBatchMoveSQLReader;
    FDBatchMoveSQLWriter1: TFDBatchMoveSQLWriter;
    FDConnection1: TFDConnection;
    FDQueryLojasLog: TFDQuery;
    FDBatchMoveDataSetReader1: TFDBatchMoveDataSetReader;
    queryTodasAsLojas: TFDMemTable;
    FDBatchMoveJSONWriter1: TFDBatchMoveJSONWriter;
    querylojas2: TFDMemTable;
    FDQuery1: TFDQuery;
  private
   { Private declarations }
    procedure ConfigureBatchMove(TableName: string);
    procedure ProcessarPaginas(const SQLBase: string; DiasReplicacao: Integer);
  public
    procedure MoverDadosVendasNFCe;
    procedure MoverDadosVendasNFCeDinamico(DiasReplicacao: Integer);
  end;
var
  DtmMoverDados: TDtmMoverDados;
implementation
uses
  UFBConnection, UPGConnection, UPrincipal, UPing2;
{%CLASSGROUP 'Vcl.Controls.TControl'}
{$R *.dfm}
{ TDtmMoverDados }

function GetStrNumber(const S: string): string;
var
  vText : PChar;
begin
  vText := PChar(S);
  Result := '';

  while (vText^ <> #0) do
  begin
    {$IFDEF UNICODE}
    if CharInSet(vText^, ['0'..'9']) then
    {$ELSE}
    if vText^ in ['0'..'9'] then
    {$ENDIF}
      Result := Result + vText^;

    Inc(vText);
  end;
end;

procedure TDtmMoverDados.ConfigureBatchMove(TableName: string);
begin
  FDBatchMoveSQLWriter1.Connection :=  DtmFBConnection.FDConnection1;
  FDBatchMoveSQLWriter1.TableName := TableName;
  FDBatchMove1.Mode := dmAppend;
end;

procedure TDtmMoverDados.ProcessarPaginas(const SQLBase: string; DiasReplicacao: Integer);
var
  Offset, Limit: Integer;
begin
  Limit := 1000;  // Ajuste conforme necessário
  Offset := 0;
  DtmPGConnection.FDQuery1.Close;
  queryTodasAsLojas.Close;
  DtmPGConnection.FDQuery1.SQL.Text := SQLBase;
  DtmPGConnection.FDQuery1.ParamByName('dias').AsInteger := DiasReplicacao;
  DtmFBConnection.FDConnection1.StartTransaction;
  try
    repeat
      DtmPGConnection.FDQuery1.ParamByName('limit').AsInteger := Limit;
      DtmPGConnection.FDQuery1.ParamByName('offset').AsInteger := Offset;
      DtmPGConnection.FDQuery1.Open;
      if not DtmPGConnection.FDQuery1.Eof then
      begin
        queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.Data;
        FDBatchMoveDataSetReader1.DataSet := queryTodasAsLojas;
        FDBatchMove1.Execute;
      end;
      Offset := Offset + Limit;
      FDQuery1.Close;
    until FDQuery1.RecordCount < Limit;
    DtmFBConnection.FDConnection1.Commit;
  except
    DtmFBConnection.FDConnection1.Rollback;
    raise;
  end;
end;


procedure TDtmMoverDados.MoverDadosVendasNFCeDinamico(DiasReplicacao: Integer);
const
  SQLBase = 'SELECT * FROM EST_MVTO_LINX WHERE dta_transacao >= CURRENT_DATE - :dias LIMIT :limit OFFSET :offset';
begin
  ConfigureBatchMove('EST_MVTO_LINX');
  ProcessarPaginas(SQLBase, DiasReplicacao);
end;

procedure TDtmMoverDados.MoverDadosVendasNFCe;
  var
    MyText: TStringlist;
    sDiasNotas: String;
    sDiasRecebimento: String;
    sDiasXMLLoja: String;
    sDiasProtocolo: String;
    sIPLoja : String;
    Counter: Integer;
    bHabilitaUpdate: Boolean;
  begin
    if ((StrToTime(TimeToStr(now)) >= StrToTime('00:00:00') ) and
        (StrToTime(TimeToStr(now)) <= StrToTime('03:00:00') )) then
    begin
         Exit;
    end;
    bHabilitaUpdate := True;
    DtmPGConnection.FDConnection1.Connected := False;
    DtmFBConnection.FDConnection1.Connected := False;
    if ((StrToTime(TimeToStr(now)) >= StrToTime('01:05:00') ) and
        (StrToTime(TimeToStr(now)) <= StrToTime('07:00:00') )) then
    begin
          // sDIAS_ATUALIZACAO := '7';
          FrmPrincipal.edtDiasReplicacao.text := '9';
          sDiasNotas := '9';
          sDiasXMLLoja := '9';
          sDiasProtocolo := '9';
          sDiasRecebimento := '9';
    end
    else
    begin
          // sDIAS_ATUALIZACAO := '0';
          FrmPrincipal.edtDiasReplicacao.text := '0';
          sDiasNotas := '0';
          sDiasXMLLoja := '0';
          sDiasProtocolo := '0';
          sDiasRecebimento := '0';
    end;
    // System.IOUtils.TFile.Copy('C:\sislog\dados_servidor\vendas_nfce.FDB','C:\sislog\dados_servidor\vendas_temp.FDB',true);
    sleep(1000);
    try
      try
        MyText := TStringlist.create;
        if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
          MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        MyText.Add('inicalizado replicacao oracle ' + DateTimeToStr(now));
        MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
      except
        // segue a execu��o
      end;
    finally
      MyText.Free
    end;
    try
      try
        MyText := TStringlist.create;
        if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
          MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        MyText.Add('Maquina  ' +FrmPrincipal.sNomeDaMaquina+ ' ' + DateTimeToStr(now));
        MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
      except
        // segue a execu��o
      end;
    finally
      MyText.Free
    end;

    try
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      //DtmFBConnection.FDConnection1.Params.CharacterSet := 'UTF8';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
    except
          //segue
    end;
    DtmMoverDados.FDQueryLojasLog.active := False;
    DtmMoverDados.FDQueryLojasLog.SQL.text := ' select IP_LOJA from NL.grz_lojas_tamanho_base where cod_unidade = :COD_UNIDADE ';
    DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
    DtmMoverDados.FDQueryLojasLog.active := True;
    if DtmMoverDados.FDQueryLojasLog.RecordCount >0 then
    begin
         sIPLoja := Trim((DtmMoverDados.FDQueryLojasLog.FieldByName('IP_LOJA').AsString));
    end
    else
    begin
         sIPLoja := Trim(GetStrNumber(FrmPrincipal.sDatabase_Serv));
    end;
    DtmMoverDados.FDQueryLojasLog.active := False;

    //MoverDadosVendasNFCeDinamico(2);
    if not PingHost( sIPLoja ) then
    begin
          DtmMoverDados.FDQueryLojasLog.active := False;
          DtmMoverDados.FDQueryLojasLog.SQL.text := ' select * from SISLOGWEB.GRZ_MVTO_integracoes B  '+
                                                    ' where cod_unidade = :COD_UNIDADE '+
                                                    ' and dta_sistema >= sysdate -0.05     '+
                                                    ' and des_integracao = ''INTERNET_WHATS'' '+
                                                    ' and des_execucao like ''%OFFLINE%'' ';
          DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
          DtmMoverDados.FDQueryLojasLog.active := True;
          if DtmMoverDados.FDQueryLojasLog.RecordCount = 0  then
          begin
                DtmMoverDados.FDQueryLojasLog.active := False;
                DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                                        + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
                DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
                DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
                DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'INTERNET_WHATS';
                DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' LOJA SEM INTERNET OFFLINE ' + DateTimeToStr(now);
                DtmMoverDados.FDQueryLojasLog.execsql;
                DtmMoverDados.FDQueryLojasLog.Connection.commit;
                DtmFBConnection.FDConnection1.Connected := False;
          end;

          DtmMoverDados.FDQueryLojasLog.active := False;
          DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                                  + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
          DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
          DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
          DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'INTERNET';
          DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' LOJA SEM INTERNET OFFLINE ' + DateTimeToStr(now);
          DtmMoverDados.FDQueryLojasLog.execsql;
          DtmMoverDados.FDQueryLojasLog.Connection.commit;
          DtmFBConnection.FDConnection1.Connected := False;
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add('LOJA SEM INTERNET OFFLINE ' + DateTimeToStr(now));
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;

    end
    else
    begin
          DtmMoverDados.FDQueryLojasLog.active := False;
          DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                                  + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
          DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
          DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
          DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'INTERNET';
          DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' INTERNET OK ONLINE ' + DateTimeToStr(now);
          DtmMoverDados.FDQueryLojasLog.execsql;
          DtmMoverDados.FDQueryLojasLog.Connection.commit;
          DtmFBConnection.FDConnection1.Connected := False;
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add('INTERNET OK ONLINE ' + DateTimeToStr(now));
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;

    end;

    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' SELECT C.* FROM GRZ_CONFIGURACOES_INTEGRACOES C  WHERE COD_EMP = :COD_EMP AND COD_UNIDADE = :COD_UNIDADE ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.active := True;
      if DtmMoverDados.FDQueryLojasLog.RecordCount > 0 then
      begin
            FrmPrincipal.edtDiasReplicacao.text := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_REPLICACAO').AsString);
            sDiasNotas := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_REPLICACAO_NOTAS').AsString);
            sDiasXMLLoja := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_REPLICACAO_XML_NFCE').AsString);
            sDiasProtocolo  := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_PROTOCOLO_NFCE').AsString);
            sDiasRecebimento := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_REPLICACAO_RECEB').AsString);
      end else
      begin
        DtmMoverDados.FDQueryLojasLog.active := False;
        DtmMoverDados.FDQueryLojasLog.SQL.text := ' SELECT C.* FROM GRZ_CONFIGURACOES_INTEGRACOES C  WHERE COD_EMP = :COD_EMP AND COD_UNIDADE = :COD_UNIDADE ';
        DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat     := 999;
        DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := 999;
        DtmMoverDados.FDQueryLojasLog.active := True;
        if DtmMoverDados.FDQueryLojasLog.RecordCount > 0 then
        begin
              FrmPrincipal.edtDiasReplicacao.text := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_REPLICACAO').AsString);
              sDiasNotas := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_REPLICACAO_NOTAS').AsString);
              sDiasXMLLoja := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_REPLICACAO_XML_NFCE').AsString);
              sDiasProtocolo  := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_PROTOCOLO_NFCE').AsString);
              sDiasRecebimento := Trim(DtmMoverDados.FDQueryLojasLog.FieldByName('DIAS_REPLICACAO_RECEB').AsString);
        end else
        begin
             FrmPrincipal.edtDiasReplicacao.text := '0';
             sDiasXMLLoja := '0';
             sDiasProtocolo:= '0';
        end;
      end;
    except
      on E: Exception do
      begin
        // Memo1.Lines.Add(E.Message);
      end;
    end;
    // CONEXao FIREBIRD
    DtmPGConnection.FDConnection1.Params.database := FrmPrincipal.sDatabase_Serv;
    DtmPGConnection.FDConnection1.Params.USERNAME := 'grazz';
    DtmPGConnection.FDConnection1.Params.Password := 'sisloggrazz';
    DtmPGConnection.FDConnection1.Connected := True;
    DtmFBConnection.FDConnection1.Connected := False;
    if ((StrToTime(TimeToStr(now)) >= StrToTime('01:05:00') ) and
        (StrToTime(TimeToStr(now)) <= StrToTime('07:00:00') )) then
    begin
      // sDIAS_ATUALIZACAO := '7';
      FrmPrincipal.edtDiasReplicacao.text := '9';
      sDiasNotas  := '9';
      sDiasXMLLoja := '9';
    end;
    try
      try
        MyText := TStringlist.create;
        if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
          MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        MyText.Add('Replicando '+ FrmPrincipal.edtDiasReplicacao.text+' dias venda hora: ' + DateTimeToStr(now));
        MyText.Add('Replicando '+ sDiasNotas+' dias NFe hora: ' + DateTimeToStr(now));
        MyText.Add('Replicando '+ sDiasXMLLoja+' dias XML hora: ' + DateTimeToStr(now));
        MyText.Add('Replicando '+ sDiasRecebimento+' dias Recebimento hora: ' + DateTimeToStr(now));

        MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
      except
        // segue a execu��o
      end;
    finally
      MyText.Free
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' VAI INICIAR REPLICACAO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
      begin
        // Memo1.Lines.Add(E.Message);
      end;
    end;
    {
    try
    FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
    FDBatchMoveSQLReader1.ReadSQL := ' select * from GER_LAYOUTS ';
    FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
    FDBatchMoveSQLWriter1.TableName := 'GER_LAYOUTS';
    FDBatchMove1.Mode := dmAppend;
    FDBatchMove1.Execute;
    except
      on E: Exception do
      begin
           ShowMessage(E.Message);
      end;


    end;

    }

    //MoverDadosVendasNFCeDinamico(2);

    Counter := 0;
    // Executa o loop enquanto Counter for menor que DiasReplicacao
   { while Counter < StrToIntDef( (FrmPrincipal.edtDiasReplicacao.text),0)  do
    begin
      }
            try


              DtmPGConnection.FDQuery1.Close;
              queryTodasAsLojas.Close;
              DtmPGConnection.FDQuery1.Active := False;
              DtmPGConnection.FDQuery1.sql.Text :=    ' select  * from EST_NFCE_CONTROLE  where DTA_MOVIMENTO >= current_date - ' + Inttostr(Counter) ;
              DtmPGConnection.FDQuery1.Active := True;
              DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
              DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_NFCE_CONTROLE.json', sfJSON);
             // DtmPGConnection.FDQuery1.Active := False;
              //DtmFBConnection.FDConnection1.Offline;

              queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
              FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
              FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
              FDBatchMoveSQLWriter1.TableName := 'EST_NFCE_CONTROLE';
              //FDBatchMoveSQLWriter1.WriteSQL
              FDBatchMove1.Mode := dmAlwaysInsert;
              FDBatchMove1.Execute;

              try
                try
                  MyText := TStringlist.create;
                  if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                    MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                    MyText.Add('Replicou EST_NFCE_CONTROLE ' + DateTimeToStr(now));
                  MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                except
                  // segue
                end;
              finally
                MyText.Free
              end;
            except
              on E: Exception do
              begin
                try
                  try
                    MyText := TStringlist.create;
                    if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                      MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                    MyText.Add(E.Message);
                    MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                  except
                    // segue
                  end;
                finally
                  MyText.Free
                end;
              end;
            end;


     //  Inc(Counter);
    //end;

   // Counter := 0;
    // Executa o loop enquanto Counter for menor que DiasReplicacao
   { while Counter < StrToIntDef( (FrmPrincipal.edtDiasReplicacao.text),0)  do
    begin
    }
         try

              DtmPGConnection.FDQuery1.Close;
              queryTodasAsLojas.Close;
              DtmPGConnection.FDQuery1.Active := False;
              DtmPGConnection.FDQuery1.sql.Text :=    ' select * from EST_MVTO_LINX  where dta_transacao  >= current_date - ' + Inttostr(Counter) ;
              DtmPGConnection.FDQuery1.Active := True;
              DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
              DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_MVTO_LINX.json', sfJSON);
              //DtmPGConnection.FDQuery1.Active := False;
              //DtmFBConnection.FDConnection1.Offline;
              queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
              FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
              FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
              FDBatchMoveSQLWriter1.TableName := 'EST_MVTO_LINX';
              //FDBatchMoveSQLWriter1.WriteSQL
              FDBatchMove1.Mode := dmAlwaysInsert;
              FDBatchMove1.Execute;

              try
                try
                  MyText := TStringlist.create;
                  if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                    MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                  MyText.Add('Replicou  EST_MVTO_LINX  ' + DateTimeToStr(now));
                  MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                except
                  // segue
                end;
              finally
                MyText.Free
              end;
            except
              on E: Exception do
              begin
                try
                  try
                    MyText := TStringlist.create;
                    if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                      MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                    MyText.Add(E.Message);
                    MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                  except
                    // segue
                  end;
                finally
                  MyText.Free
                end;
              end;
            end;




















    try


      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=    ' select * from GER_MVTO_BONUS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_MVTO_BONUS.json', sfJSON);
      //DtmPGConnection.FDQuery1.Active := False;
      //DtmFBConnection.FDConnection1.Offline;
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_MVTO_BONUS';

      //FDBatchMoveSQLWriter1.WriteSQL
      FDBatchMove1.Mode := dmAlwaysInsert;
      FDBatchMove1.Execute;
      DtmPGConnection.FDQuery1.Active := False;
      DtmFBConnection.FDConnection1.Connected := False;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      //ShowMessage(FrmPrincipal.sNomeDaMaquina);
      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from GER_MVTO_BONUS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_MVTO_BONUS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;

      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from GER_MVTO_BONUS  where DTA_MOVIMENTO >= current_date - ' +  (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_MVTO_BONUS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_MVTO_BONUS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue
          end;
        finally
          MyText.Free
        end;
      end;
    end;


    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=    ' select * from EST_INVENTARIO_ITENS_VOL  where DTA_INVENTARIO >= current_date - 360 ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_INVENTARIO_ITENS_VOL.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      DtmFBConnection.FDConnection1.Offline;

           {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_INVENTARIO_ITENS_VOL';
      DtmFBConnection.FDConnection1.Connected := False;
      //FDBatchMoveSQLWriter1.WriteSQL
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
          }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_INVENTARIO_ITENS_VOL ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue
          end;
        finally
          MyText.Free
        end;
      end;
    end;


    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=    ' select * from EST_INVENTARIO_VOLUMES  where DTA_INVENTARIO >= current_date - 360 ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_INVENTARIO_VOLUMES.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      DtmFBConnection.FDConnection1.Offline;

       {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_INVENTARIO_VOLUMES';
      DtmFBConnection.FDConnection1.Connected := False;
      //FDBatchMoveSQLWriter1.WriteSQL
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_INVENTARIO_VOLUMES ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue
          end;
        finally
          MyText.Free
        end;
      end;
    end;


    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=    ' select * from EST_INVENTARIO_CABECALHO  where DTA_INVENTARIO >= current_date - 360 ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_INVENTARIO_CABECALHO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      DtmFBConnection.FDConnection1.Offline;
      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_INVENTARIO_CABECALHO';
      DtmFBConnection.FDConnection1.Connected := False;
      //FDBatchMoveSQLWriter1.WriteSQL
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }

      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_INVENTARIO_CABECALHO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue
          end;
        finally
          MyText.Free
        end;
      end;
    end;


    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=    ' select COD_EMP                     '+
                                              '       ,COD_UNIDADE                 '+
                                              '       ,DTA_MOVIMENTO               '+
                                              '       ,COD_CLIENTE                 '+
                                              '       ,NUM_SEQ                     '+
                                              '       ,DES_COMENTARIO              '+
                                              '       ,COD_NIVEL                   '+
                                              '       ,DATA_ULT_ALTERACAO          '+
                                              '       ,REP_STATUS                  '+
                                              '       ,IND_MOTIVO_CONSULTA_SPC     '+
                                              ' from CRE_COMENTARIOS_CLI_MVTO      '+
                                              '  where dta_movimento >= current_date - 60 '+
                                              ' order by                           '+
                                              ' COD_EMP,                           '+
                                              ' COD_UNIDADE,                       '+
                                              ' DTA_MOVIMENTO,                     '+
                                              ' COD_CLIENTE,                       '+
                                              ' NUM_SEQ                            ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_COMENTARIOS_CLI_MVTO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      DtmFBConnection.FDConnection1.Offline;
          {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_COMENTARIOS_CLI_MVTO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
         }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=  ' select COD_EMP                     '+
                                        '       ,COD_UNIDADE                 '+
                                        '       ,DTA_MOVIMENTO               '+
                                        '       ,COD_CLIENTE                 '+
                                        '       ,NUM_SEQ                     '+
                                        '       ,DES_COMENTARIO              '+
                                        '       ,COD_NIVEL                   '+
                                        '       ,DATA_ULT_ALTERACAO          '+
                                        '       ,REP_STATUS                  '+
                                        '       ,IND_MOTIVO_CONSULTA_SPC     '+
                                        ' from CRE_COMENTARIOS_CLI_MVTO      '+
                                        '  where dta_movimento >= current_date - 60 '+
                                        ' order by                           '+
                                        ' COD_EMP,                           '+
                                        ' COD_UNIDADE,                       '+
                                        ' DTA_MOVIMENTO,                     '+
                                        ' COD_CLIENTE,                       '+
                                        ' NUM_SEQ                            ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_COMENTARIOS_CLI_MVTO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP                     '+
                                        '       ,COD_UNIDADE                 '+
                                        '       ,DTA_MOVIMENTO               '+
                                        '       ,COD_CLIENTE                 '+
                                        '       ,NUM_SEQ                     '+
                                        '       ,DES_COMENTARIO              '+
                                        '       ,COD_NIVEL                   '+
                                        '       ,DATA_ULT_ALTERACAO          '+
                                        '       ,REP_STATUS                  '+
                                        '       ,IND_MOTIVO_CONSULTA_SPC     '+
                                        ' from CRE_COMENTARIOS_CLI_MVTO      '+
                                        '  where dta_movimento >= current_date - 60 '+
                                        ' order by                           '+
                                        ' COD_EMP,                           '+
                                        ' COD_UNIDADE,                       '+
                                        ' DTA_MOVIMENTO,                     '+
                                        ' COD_CLIENTE,                       '+
                                        ' NUM_SEQ                            ';

      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_COMENTARIOS_CLI_MVTO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_COMENTARIOS_CLI_MVTO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue
          end;
        finally
          MyText.Free
        end;
      end;
    end;

    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' Replicou Bonus ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
      begin
        // Memo1.Lines.Add(E.Message);
      end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=   ' select * from EST_MVTO_SEGURO  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_MVTO_SEGURO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_MVTO_SEGURO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_MVTO_SEGURO  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_MVTO_SEGURO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_MVTO_SEGURO  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_MVTO_SEGURO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }

      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_MVTO_SEGURO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue
          end;
        finally
          MyText.Free
        end;
      end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  ' Replicou seguro ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
      begin
        // Memo1.Lines.Add(E.Message);
      end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=  ' select * from GER_CONTROLE_VERSAO ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_CONTROLE_VERSAO_LOJAS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

             {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_CONTROLE_VERSAO_LOJAS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
            }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from GER_CONTROLE_VERSAO ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_CONTROLE_VERSAO_LOJAS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  from GER_CONTROLE_VERSAO ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_CONTROLE_VERSAO_LOJAS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_CONTROLE_VERSAO_LOJAS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue a execu��o
          end;
        finally
          MyText.Free
        end;
      end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  ' Replicou Controle Versao ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
      begin
        // Memo1.Lines.Add(E.Message);
      end;
    end;
   { // INICIO DA REPLICA��O GER_CONFIGURACAO_DECISOR_AUT (FireBase -> ORACLE)
    try
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  SELECT COD_EMP                              ' +
        '        ,COD_UNIDADE                          ' +
        '        ,COD_PERFIL                           ' +
        '        ,MDA                                  ' +
        '        ,CREDIT_SCORE                         ' +
        '        ,MAIOR_ACUM                           ' +
        '        ,TETO                                 ' +
        '        ,SPC1                                 ' +
        '        ,SPC2                                 ' +
        '        ,IND_ATIVO                            ' +
        '        ,DTA_ALTERACAO                        ' +
        '  FROM GER_CONFIGURACAO_DECISOR_AUT           ' +
        '  ORDER BY COD_EMP,       ' + '           COD_UNIDADE    ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_CONFIGURACAO_DECISOR_AUT';
      FDBatchMove1.Mode := dmAppend; // somente faz insert
      FDBatchMove1.Execute;
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_CONFIGURACAO_DECISOR_AUT ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' Replicou GER_CONFIGURACAO_DECISOR_AUT ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    // FIM DA REPLICA��O GER_CONFIGURACAO_DECISOR_AUT (FireBase -> ORACLE)
    // FIM DA REPLICA��O GER_PERFIL (FireBase -> ORACLE)
    try
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
          ' SELECT COD_PERFIL            ' +
          '   ,FATOR_MULTIPLICADOR       ' +
          '   ,QTD_DIAS_CALCULO_CS       ' +
          '   ,VALOR_MULTIPLICADOR_CS    ' +
          '   ,PERC_RENDA_DECISOR        ' +
          '   ,QTD_DIAS_TOLERANCIA_CLI   ' +
          ' FROM GER_PERFIL              ' +
          ' ORDER BY  COD_PERFIL         ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_PERFIL';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_PERFIL ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' Replicou GER_PERFIL ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    // FIM DA REPLICA��O GER_PERFIL (FireBase -> ORACLE)
    }
    // INICIO DA REPLICA��O CRE_REFINANCIAMENTO (FireBase -> ORACLE)
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=  ' SELECT * FROM  CRE_REFINANCIAMENTO';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_REFINANCIAMENTO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

       {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_REFINANCIAMENTO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      }
      //FDBatchMove1.Mode := dmUpdate;
      //FDBatchMove1.Execute;
      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' SELECT * FROM  CRE_REFINANCIAMENTO';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_REFINANCIAMENTO';
      FDBatchMove1.Mode := dmAppend; // SOMENTE INSERT
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_REFINANCIAMENTO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // SEGUE O BAILE
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue o baile
          end;
        finally
          MyText.Free
        end;
      end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' Replicou CRE_REFINANCIAMENTO ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
      begin
        // Memo1.Lines.Add(E.Message);
      end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    // FIM DA REPLICA��O CRE_REFINANCIAMENTO (FireBase -> ORACLE)
    // INICIO DA REPLICAÇÃO GER_CONFIGURACAO_CARTAO_PRES (FireBase -> ORACLE)
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;

      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '  SELECT COD_EMP                        ' +
                                           '	     	,COD_UNIDADE                    ' +
                                           '	     	,DES_USUARIO                    ' +
                                           '	     	,DES_SENHA                      ' +
                                           '	     	,DES_UNIDADE_ID                 ' +
                                           '	     	,DES_CHAVE_ACESSO               ' +
                                           '	     	,DES_SENHA_CARTAO               ' +
                                           '	     	,VLR_MINIMO_CARTAO              ' +
                                           '	     	,VLR_MAXIMO_CARTAO              ' +
                                           '	     	,VLR_MAXIMO_TROCO               ' +
                                           '	     	,URL_SERVER_HOST                ' +
                                           '	     	,PATH_GIFT_API                  ' +
                                           '	     	,IND_ALTERA_VALOR_RECEB_CARTAO  ' +
                                           '  FROM GER_CONFIGURACAO_CARTAO_PRES     ' +
                                           ' WHERE COD_EMP         = ' + IntToStr(FrmPrincipal.iCod_Emp) +
                                           '   AND cod_unidade     = ' + IntToStr(FrmPrincipal.iCod_unidade);

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  { ABRE TODAS AS LINHAS DA CONSULTA }

      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_CONFIGURACAO_CARTAO_PRES.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      if DtmPGConnection.FDQuery1.RecordCount > 0 then
      begin
        DtmFBConnection.FDQuery1.active := False;
        DtmFBConnection.FDQuery1.SQL.text := ' SELECT 1 FROM  GER_CONFIGURACAO_CARTAO_PRES     ' +
                                             ' WHERE COD_EMP         = ' + IntToStr(FrmPrincipal.iCod_Emp) +
                                             '   AND COD_UNIDADE     = ' + IntToStr(FrmPrincipal.iCod_unidade);
        DtmFBConnection.FDQuery1.Active := True;

        if DtmFBConnection.FDQuery1.RecordCount > 0 then
        begin
          try
            DtmFBConnection.FDQuery1.active := False;
            DtmFBConnection.FDQuery1.SQL.text := ' UPDATE GER_CONFIGURACAO_CARTAO_PRES ' +
                                                      ' SET  DES_USUARIO       = :DES_USUARIO       ' +
                                                          ' ,DES_SENHA         = :DES_SENHA         ' +
                                                          ' ,DES_UNIDADE_ID    = :DES_UNIDADE_ID    ' +
                                                          ' ,DES_CHAVE_ACESSO  = :DES_CHAVE_ACESSO  ' +
                                                          ' ,DES_SENHA_CARTAO  = :DES_SENHA_CARTAO  ' +
                                                          ' ,VLR_MINIMO_CARTAO = :VLR_MINIMO_CARTAO ' +
                                                          ' ,VLR_MAXIMO_CARTAO = :VLR_MAXIMO_CARTAO ' +
                                                          ' ,VLR_MAXIMO_TROCO  = :VLR_MAXIMO_TROCO  ' +
                                                          ' ,URL_SERVER_HOST   = :URL_SERVER_HOST   ' +
                                                          ' ,PATH_GIFT_API     = :PATH_GIFT_API     ' +
                                                          ' ,IND_ALTERA_VALOR_RECEB_CARTAO = :IND_ALTERA_VALOR_RECEB_CARTAO ' +
                                                      ' WHERE COD_EMP         = ' + IntToStr(FrmPrincipal.iCod_Emp) +
                                                      '   AND COD_UNIDADE     = ' + IntToStr(FrmPrincipal.iCod_unidade);

            DtmFBConnection.FDQuery1.ParamByName('DES_USUARIO').AsString       :=  DtmPGConnection.FDQuery1.FieldByName('DES_USUARIO').AsString;
            DtmFBConnection.FDQuery1.ParamByName('DES_SENHA').AsString         :=  DtmPGConnection.FDQuery1.FieldByName('DES_SENHA').AsString;
            DtmFBConnection.FDQuery1.ParamByName('DES_UNIDADE_ID').AsString    :=  DtmPGConnection.FDQuery1.FieldByName('DES_UNIDADE_ID').AsString;
            DtmFBConnection.FDQuery1.ParamByName('DES_CHAVE_ACESSO').AsString  :=  DtmPGConnection.FDQuery1.FieldByName('DES_CHAVE_ACESSO').AsString;
            DtmFBConnection.FDQuery1.ParamByName('DES_SENHA_CARTAO').AsString  :=  DtmPGConnection.FDQuery1.FieldByName('DES_SENHA_CARTAO').AsString;
            DtmFBConnection.FDQuery1.ParamByName('VLR_MINIMO_CARTAO').AsString :=  DtmPGConnection.FDQuery1.FieldByName('VLR_MINIMO_CARTAO').AsString;
            DtmFBConnection.FDQuery1.ParamByName('VLR_MAXIMO_CARTAO').AsString :=  DtmPGConnection.FDQuery1.FieldByName('VLR_MAXIMO_CARTAO').AsString;
            DtmFBConnection.FDQuery1.ParamByName('VLR_MAXIMO_TROCO').AsString  :=  DtmPGConnection.FDQuery1.FieldByName('VLR_MAXIMO_TROCO').AsString;
            DtmFBConnection.FDQuery1.ParamByName('URL_SERVER_HOST').AsString   :=  DtmPGConnection.FDQuery1.FieldByName('URL_SERVER_HOST').AsString;
            DtmFBConnection.FDQuery1.ParamByName('PATH_GIFT_API').AsString     :=  DtmPGConnection.FDQuery1.FieldByName('PATH_GIFT_API').AsString;
            DtmFBConnection.FDQuery1.ParamByName('IND_ALTERA_VALOR_RECEB_CARTAO').AsString := DtmPGConnection.FDQuery1.FieldByName('IND_ALTERA_VALOR_RECEB_CARTAO').AsString;

            DtmFBConnection.FDQuery1.execsql;
            DtmFBConnection.FDQuery1.Connection.commit;
            DtmFBConnection.FDConnection1.Connected := False;
          except
            on E: Exception do
            begin
              // Memo1.Lines.Add(E.Message);
            end;
          end;
        end
        else
        begin
          queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
          DtmPGConnection.FDQuery1.Active := False;
          FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
          FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
          FDBatchMoveSQLWriter1.TableName := 'GER_CONFIGURACAO_CARTAO_PRES';
          DtmFBConnection.FDConnection1.Connected := False;
          FDBatchMove1.Mode := dmAppend;  { INSERT }
         // FDBatchMove1.Execute;
      //  end;
      //end;

      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_CONFIGURACAO_CARTAO_PRES ' +  DateTimeToStr(now));

          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          { segue a execução }
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);

            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            { segue a execução }
          end;
        finally
          MyText.Free
        end;
      end;
    end;
    try
      // CONEXÃO ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;

      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' Replicou GER_CONFIGURACAO_CARTAO_PRES ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    // FIM DA REPLICA��O GER_CONFIGURACAO_CARTAO_PRES (FireBase -> ORACLE)
     // INICIO DA REPLICAÇÃO GER_MVTO_DECISOR (FireBase -> ORACLE)
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '  SELECT * FROM GER_MVTO_DECISOR WHERE DTA_MOVIMENTO >= current_date -15 ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_MVTO_DECISOR.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      DtmPGConnection.FDQuery1.RecordCount;
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_MVTO_DECISOR';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;  { SOMENTE INSERT }
     { FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_MVTO_DECISOR ' +  DateTimeToStr(now));

          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          { segue a execução }
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' +  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);

            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            { segue a execução }
          end;
        finally
          MyText.Free
        end;
      end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;

      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' Replicou GER_MVTO_DECISOR ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' Replicou CRE_COBRANCA_CLI_MVTO ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
      begin
        // Memo1.Lines.Add(E.Message);
      end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=   ' select * from CRE_CONTAS_RECEBER  where DTA_VENDA >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_CONTAS_RECEBER.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CONTAS_RECEBER';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end; }

      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from CRE_CONTAS_RECEBER  where DTA_VENDA >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CONTAS_RECEBER';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from CRE_CONTAS_RECEBER  where DTA_VENDA >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CONTAS_RECEBER';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_CONTAS_RECEBER ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue a execu��o
          end;
        finally
          MyText.Free
        end;
      end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := ' Replicou cre_contas_receber ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
      begin
        // Memo1.Lines.Add(E.Message);
      end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=    ' select COD_EMP                                                 ' +
        '      ,COD_UNIDADE                                              ' +
        '      ,NUM_EQUIPAMENTO                                          ' +
        '      ,DTA_MOVIMENTO                                            ' +
        '      ,NUM_CUPOM                                                ' +
        '      ,COD_OPERACAO                                             ' +
        '      ,NUM_SEQ_ITEM                                             ' +
        '      ,COD_ITEM                                                 ' +
        '      ,COD_ESTRUTURADO                                          ' +
        '      ,HOR_MOVIMENTO                                            ' +
        '      ,SEQ_CUPOM                                                ' +
        '      ,QTD_MOVIMENTO                                            ' +
        '      ,VLR_UNITARIO                                             ' +
        '      ,VLR_DESCTO_ITEM                                          ' +
        '      ,VLR_DESCTO_CUPOM                                         ' +
        '      ,VLR_ACRESCIMO                                            ' +
        '      ,VLR_TOTAL                                                ' +
        '      ,VLR_LIQUIDO                                              ' +
        '      ,TIP_PRECO                                                ' +
        '      ,COD_LISTA                                                ' +
        '      ,COD_DESCONTO                                             ' +
        '      ,COD_GR_FISCAL                                            ' +
        '      ,DES_TOTALIZ_FISCAL                                       ' +
        '      ,NUM_CFOP                                                 ' +
        '      ,PER_ICMS                                                 ' +
        '      ,VLR_BC_ICMS                                              ' +
        '      ,VLR_ICMS                                                 ' +
        '      ,VLR_ST                                                   ' +
        '      ,VLR_PIS                                                  ' +
        '      ,VLR_COFINS                                               ' +
        '      ,DTA_SISTEMA                                              ' +
        '      ,0 as  IND_STATUS                                         ' +
        '      ,VLR_PRODUTO                                              ' +
        '      ,VLR_ACRESCIMO_COB                                        ' +
        '      ,VLR_OUTRAS                                               ' +
        '      ,VLR_ISENTAS                                              ' +
        '      ,PER_PIS                                                  ' +
        '      ,PER_COFINS                                               ' +
        '      ,IND_CANCELADO                                            ' +
        '      ,COD_PROMOCAO                                             ' +
        '      ,PER_ALIQ_IMPOSTOS                                        ' +
        '      ,VLR_IMPOSTOS                                             ' +
        '      ,COD_NCM                                                  ' +
        '      ,PER_ALIQ_EST                                             ' +
        '      ,PER_ALIQ_MUN                                             ' +
        '      ,VLR_IMPOSTOS_EST                                         ' +
        '      ,VLR_IMPOSTOS_MUN                                         ' +
        '      ,COD_CEST                                                 ' +
        '      ,COD_EMP_ITEM                                             ' +
        '      ,PER_ICMS_PREV                                            ' +
        '      ,VLR_BC_PREV                                              ' +
        '      ,VLR_ICMS_PREV                                            ' +
        '      ,COD_BENEFICIO                                            ' +
        '      ,COD_MOT_DES_ICMS                                         ' +
        '      ,VLR_ICMS_DESON                                           ' +
        '      ,NUM_SEQ_ITEM_XML                                         ' +
        '      ,PER_REDUCAO_BC_ICMS AS  PER_REDC_BC_ICMS                 ' +
        '      ,BARRAS_CARTAO_PRESENTE AS BARRAS_CARTAO_PRES             ' +
        '      ,NUM_AUTORIZACAO_CARTAO_PRESENTE AS NUM_AUTO_CARTAO_PRES  ' +
        '      ,NUM_NSU_HOST                                             ' +
        '      ,VERSAO_SISTEMA                                           ' +
        '      ,NUM_IMEI                                                 ' +
        '      ,VLR_PRODUTO_SEGURO                                       ' +
        ' from EST_CUPOM_ITENS  where DTA_MOVIMENTO >= current_date -    ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CUPOM_ITENS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {

      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_ITENS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
       }
     { FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        ' select COD_EMP                                                 ' +
        '      ,COD_UNIDADE                                              ' +
        '      ,NUM_EQUIPAMENTO                                          ' +
        '      ,DTA_MOVIMENTO                                            ' +
        '      ,NUM_CUPOM                                                ' +
        '      ,COD_OPERACAO                                             ' +
        '      ,NUM_SEQ_ITEM                                             ' +
        '      ,COD_ITEM                                                 ' +
        '      ,COD_ESTRUTURADO                                          ' +
        '      ,HOR_MOVIMENTO                                            ' +
        '      ,SEQ_CUPOM                                                ' +
        '      ,QTD_MOVIMENTO                                            ' +
        '      ,VLR_UNITARIO                                             ' +
        '      ,VLR_DESCTO_ITEM                                          ' +
        '      ,VLR_DESCTO_CUPOM                                         ' +
        '      ,VLR_ACRESCIMO                                            ' +
        '      ,VLR_TOTAL                                                ' +
        '      ,VLR_LIQUIDO                                              ' +
        '      ,TIP_PRECO                                                ' +
        '      ,COD_LISTA                                                ' +
        '      ,COD_DESCONTO                                             ' +
        '      ,COD_GR_FISCAL                                            ' +
        '      ,DES_TOTALIZ_FISCAL                                       ' +
        '      ,NUM_CFOP                                                 ' +
        '      ,PER_ICMS                                                 ' +
        '      ,VLR_BC_ICMS                                              ' +
        '      ,VLR_ICMS                                                 ' +
        '      ,VLR_ST                                                   ' +
        '      ,VLR_PIS                                                  ' +
        '      ,VLR_COFINS                                               ' +
        '      ,DTA_SISTEMA                                              ' +
        '      ,0 as  IND_STATUS                                         ' +
        '      ,VLR_PRODUTO                                              ' +
        '      ,VLR_ACRESCIMO_COB                                        ' +
        '      ,VLR_OUTRAS                                               ' +
        '      ,VLR_ISENTAS                                              ' +
        '      ,PER_PIS                                                  ' +
        '      ,PER_COFINS                                               ' +
        '      ,IND_CANCELADO                                            ' +
        '      ,COD_PROMOCAO                                             ' +
        '      ,PER_ALIQ_IMPOSTOS                                        ' +
        '      ,VLR_IMPOSTOS                                             ' +
        '      ,COD_NCM                                                  ' +
        '      ,PER_ALIQ_EST                                             ' +
        '      ,PER_ALIQ_MUN                                             ' +
        '      ,VLR_IMPOSTOS_EST                                         ' +
        '      ,VLR_IMPOSTOS_MUN                                         ' +
        '      ,COD_CEST                                                 ' +
        '      ,COD_EMP_ITEM                                             ' +
        '      ,PER_ICMS_PREV                                            ' +
        '      ,VLR_BC_PREV                                              ' +
        '      ,VLR_ICMS_PREV                                            ' +
        '      ,COD_BENEFICIO                                            ' +
        '      ,COD_MOT_DES_ICMS                                         ' +
        '      ,VLR_ICMS_DESON                                           ' +
        '      ,NUM_SEQ_ITEM_XML                                         ' +
        '      ,PER_REDUCAO_BC_ICMS AS  PER_REDC_BC_ICMS                 ' +
        '      ,BARRAS_CARTAO_PRESENTE AS BARRAS_CARTAO_PRES             ' +
        '      ,NUM_AUTORIZACAO_CARTAO_PRESENTE AS NUM_AUTO_CARTAO_PRES  ' +
        '      ,NUM_NSU_HOST                                             ' +
        '      ,VERSAO_SISTEMA                                           ' +
        '      ,NUM_IMEI                                                 ' +
        '      ,VLR_PRODUTO_SEGURO                                       ' +
        ' from EST_CUPOM_ITENS  where DTA_MOVIMENTO >= current_date -    ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_ITENS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        ' select COD_EMP                                                 ' +
        '      ,COD_UNIDADE                                              ' +
        '      ,NUM_EQUIPAMENTO                                          ' +
        '      ,DTA_MOVIMENTO                                            ' +
        '      ,NUM_CUPOM                                                ' +
        '      ,COD_OPERACAO                                             ' +
        '      ,NUM_SEQ_ITEM                                             ' +
        '      ,COD_ITEM                                                 ' +
        '      ,COD_ESTRUTURADO                                          ' +
        '      ,HOR_MOVIMENTO                                            ' +
        '      ,SEQ_CUPOM                                                ' +
        '      ,QTD_MOVIMENTO                                            ' +
        '      ,VLR_UNITARIO                                             ' +
        '      ,VLR_DESCTO_ITEM                                          ' +
        '      ,VLR_DESCTO_CUPOM                                         ' +
        '      ,VLR_ACRESCIMO                                            ' +
        '      ,VLR_TOTAL                                                ' +
        '      ,VLR_LIQUIDO                                              ' +
        '      ,TIP_PRECO                                                ' +
        '      ,COD_LISTA                                                ' +
        '      ,COD_DESCONTO                                             ' +
        '      ,COD_GR_FISCAL                                            ' +
        '      ,DES_TOTALIZ_FISCAL                                       ' +
        '      ,NUM_CFOP                                                 ' +
        '      ,PER_ICMS                                                 ' +
        '      ,VLR_BC_ICMS                                              ' +
        '      ,VLR_ICMS                                                 ' +
        '      ,VLR_ST                                                   ' +
        '      ,VLR_PIS                                                  ' +
        '      ,VLR_COFINS                                               ' +
        '      ,DTA_SISTEMA                                              ' +
        '      ,0 as IND_STATUS                                          ' +
        '      ,VLR_PRODUTO                                              ' +
        '      ,VLR_ACRESCIMO_COB                                        ' +
        '      ,VLR_OUTRAS                                               ' +
        '      ,VLR_ISENTAS                                              ' +
        '      ,PER_PIS                                                  ' +
        '      ,PER_COFINS                                               ' +
        '      ,IND_CANCELADO                                            ' +
        '      ,COD_PROMOCAO                                             ' +
        '      ,PER_ALIQ_IMPOSTOS                                        ' +
        '      ,VLR_IMPOSTOS                                             ' +
        '      ,COD_NCM                                                  ' +
        '      ,PER_ALIQ_EST                                             ' +
        '      ,PER_ALIQ_MUN                                             ' +
        '      ,VLR_IMPOSTOS_EST                                         ' +
        '      ,VLR_IMPOSTOS_MUN                                         ' +
        '      ,COD_CEST                                                 ' +
        '      ,COD_EMP_ITEM                                             ' +
        '      ,PER_ICMS_PREV                                            ' +
        '      ,VLR_BC_PREV                                              ' +
        '      ,VLR_ICMS_PREV                                            ' +
        '      ,COD_BENEFICIO                                            ' +
        '      ,COD_MOT_DES_ICMS                                         ' +
        '      ,VLR_ICMS_DESON                                           ' +
        '      ,NUM_SEQ_ITEM_XML                                         ' +
        '      ,PER_REDUCAO_BC_ICMS AS  PER_REDC_BC_ICMS                 ' +
        '      ,BARRAS_CARTAO_PRESENTE AS BARRAS_CARTAO_PRES             ' +
        '      ,NUM_AUTORIZACAO_CARTAO_PRESENTE AS NUM_AUTO_CARTAO_PRES  ' +
        '      ,NUM_NSU_HOST                                             ' +
        '      ,VERSAO_SISTEMA                                           ' +
        '      ,NUM_IMEI                                                 ' +
        '      ,VLR_PRODUTO_SEGURO                                       ' +
        ' from EST_CUPOM_ITENS  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_ITENS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CUPOM_ITENS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
      begin
        try
          try
            MyText := TStringlist.create;
            if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
              MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            MyText.Add(E.Message);
            MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          except
            // segue a execu��o
          end;
        finally
          MyText.Free
        end;
      end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=  ' select * from EST_CUPOM_ITENS_CANCELADOS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CUPOM_ITENS_CANCELADOS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_ITENS_CANC';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

         }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPOM_ITENS_CANCELADOS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_ITENS_CANC';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPOM_ITENS_CANCELADOS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_ITENS_CANC';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CUPOM_ITENS_CANC ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=  ' select COD_EMP                                   ' +
        '       ,COD_UNIDADE                               ' +
        '       ,NUM_EQUIPAMENTO                           ' +
        '       ,DTA_MOVIMENTO                             ' +
        '       ,NUM_CUPOM                                 ' +
        '       ,COD_OPERACAO                              ' +
        '       ,HOR_MOVIMENTO                             ' +
        '       , CASE NUM_CARTAO                          ' +
        '        WHEN '''' THEN 0                          ' +
        '        WHEN null THEN 0                          ' +
        '        ELSE NUM_CARTAO                           ' +
        '        END AS  NUM_CARTAO                           ' +
        '       ,NUM_PARCELAS                              ' +
        '       ,VLR_LCTO                                  ' +
        '       , ''0''||COD_AUTORIZACAO as COD_AUTORIZACAO  ' +
        '       ,DES_CLIENTE                               ' +
        '       ,NUM_DCTO                                  ' +
        '       ,DTA_SISTEMA                               ' +
        '       ,IND_STATUS                                ' +
        '       ,IND_CANCELADO                             ' +
        '       ,COD_CANCELAMENTO                          ' +
        '       ,TIP_ORIGEM                                ' +
        '       ,REDE                                      ' +
        '       ,BANDEIRA                                  ' +
        '       ,NSUSITEF                                  ' +
        '       ,NSUHOST                                   ' +
        '       ,BIN                                       ' +
        '       ,NUM_EQUIPAMENTO_ORIG                      ' +
        '       ,IND_MANUAL                                ' +
        '       ,IND_ORIGEM                                ' +
        '       ,TIP_CARTAO                                ' +
        '       ,TIP_TRANSACAO                             ' +
        '       ,CDC                                       ' +
        '       ,IND_DEB_CRED                              ' +
        '       ,CNPJ_AUTORIZADOR                          ' +
        '       ,IND_SEQUENCIA                             ' +
        ' from EST_CUPOM_TEF  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CUPOM_TEF.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;


      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_TEF';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      }
     { FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        ' select COD_EMP                                   ' +
        '       ,COD_UNIDADE                               ' +
        '       ,NUM_EQUIPAMENTO                           ' +
        '       ,DTA_MOVIMENTO                             ' +
        '       ,NUM_CUPOM                                 ' +
        '       ,COD_OPERACAO                              ' +
        '       ,HOR_MOVIMENTO                             ' +
        '       ,NUM_CARTAO                                ' +
        '       ,NUM_PARCELAS                              ' +
        '       ,VLR_LCTO                                  ' +
        '       , ''0''||COD_AUTORIZACAO as COD_AUTORIZACAO  ' +
        '       ,DES_CLIENTE                               ' +
        '       ,NUM_DCTO                                  ' +
        '       ,DTA_SISTEMA                               ' +
        '       ,IND_STATUS                                ' +
        '       ,IND_CANCELADO                             ' +
        '       ,COD_CANCELAMENTO                          ' +
        '       ,TIP_ORIGEM                                ' +
        '       ,REDE                                      ' +
        '       ,BANDEIRA                                  ' +
        '       ,NSUSITEF                                  ' +
        '       ,NSUHOST                                   ' +
        '       ,BIN                                       ' +
        '       ,NUM_EQUIPAMENTO_ORIG                      ' +
        '       ,IND_MANUAL                                ' +
        '       ,IND_ORIGEM                                ' +
        '       ,TIP_CARTAO                                ' +
        '       ,TIP_TRANSACAO                             ' +
        '       ,CDC                                       ' +
        '       ,IND_DEB_CRED                              ' +
        '       ,CNPJ_AUTORIZADOR                          ' +
        '       ,IND_SEQUENCIA                             ' +
        ' from EST_CUPOM_TEF  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_TEF';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        ' select COD_EMP                                   ' +
        '       ,COD_UNIDADE                               ' +
        '       ,NUM_EQUIPAMENTO                           ' +
        '       ,DTA_MOVIMENTO                             ' +
        '       ,NUM_CUPOM                                 ' +
        '       ,COD_OPERACAO                              ' +
        '       ,HOR_MOVIMENTO                             ' +
        '       ,NUM_CARTAO                                ' +
        '       ,NUM_PARCELAS                              ' +
        '       ,VLR_LCTO                                  ' +
        '       , ''0''||COD_AUTORIZACAO as COD_AUTORIZACAO  ' +
        '       ,DES_CLIENTE                               ' +
        '       ,NUM_DCTO                                  ' +
        '       ,DTA_SISTEMA                               ' +
        '       ,IND_STATUS                                ' +
        '       ,IND_CANCELADO                             ' +
        '       ,COD_CANCELAMENTO                          ' +
        '       ,TIP_ORIGEM                                ' +
        '       ,REDE                                      ' +
        '       ,BANDEIRA                                  ' +
        '       ,NSUSITEF                                  ' +
        '       ,NSUHOST                                   ' +
        '       ,BIN                                       ' +
        '       ,NUM_EQUIPAMENTO_ORIG                      ' +
        '       ,IND_MANUAL                                ' +
        '       ,IND_ORIGEM                                ' +
        '       ,TIP_CARTAO                                ' +
        '       ,TIP_TRANSACAO                             ' +
        '       ,CDC                                       ' +
        '       ,IND_DEB_CRED                              ' +
        '       ,CNPJ_AUTORIZADOR                          ' +
        '       ,IND_SEQUENCIA                             ' +
        ' from EST_CUPOM_TEF  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPOM_TEF';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CUPOM_TEF ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :='  select COD_EMP       '+
                                          '    ,COD_UNIDADE       '+
                                          '    ,NUM_EQUIPAMENTO   '+
                                          '    ,NUM_CUPOM          '+
                                          '    ,COD_BARRAS_CARTAO   '+
                                          '    ,IND_ESTORNADO    '+
                                          '    ,VLR_CARTAO       '+
                                          '    ,DES_LOTE_CARTAO   '+
                                          '    ,DTA_RETORNADO      '+
                                          '    ,NUM_NSU_HOST     '+
                                          '    ,NUM_AUTORIZACAO_DEBITO '+
                                          '    ,DTA_ESTORNADO            '+
                                          '  from EST_CARTAO_PRESENTE_RETORNADO '+
                                          ' where DTA_RETORNADO >= current_date -  ' + (FrmPrincipal.edtDiasReplicacao.text)  +
                                          '   order by        '+
                                          '   COD_EMP,        '+
                                          '  COD_UNIDADE,     '+
                                          '  NUM_EQUIPAMENTO,  '+
                                          '  NUM_CUPOM,        '+
                                          '  COD_BARRAS_CARTAO, '+
                                          '  IND_ESTORNADO     ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CARTAO_PRESENTE_RETORNADO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CARTAO_PRESENTE_RETORNADO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CARTAO_PRESENTE_RETORNADO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CUPONS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CUPONS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from CRE_RENEGOCIACAO  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_RENEGOCIACAO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_RENEGOCIACAO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_RENEGOCIACAO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue=
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from EST_VALE_TROCA_RETORNADO  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_VALE_TROCA_RETORNADO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_VALE_TROCA_RETORNADO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_VALE_TROCA_RETORNADO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue=
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from EST_MVTO_TEF where DTA_TRANSACAO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_MVTO_TEF.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_MVTO_TEF';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS  where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_MVTO_TEF ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue=
            end;
          finally
            MyText.Free
          end;
        end;
    end;

     DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=  '  select      COD_EMP    '+
                                            '      ,' + IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
                                            '      ,COD_CLIENTE                     '+
                                            '      ,DTA_PRI_COMPRA_VV               '+
                                            '      ,DTA_ULT_COMPRA_VV               '+
                                            '      ,VLR_ULT_COMPRA_VV               '+
                                            '      ,DTA_MAIOR_COMPRA_VV             '+
                                            '      ,VLR_MAIOR_COMPRA_VV             '+
                                            '      ,QTD_COMPRAS_VV                  '+
                                            '      ,DTA_ULT_COMPRA_VP               '+
                                            '      ,DTA_PRI_COMPRA_VP               '+
                                            '      ,VLR_ULT_COMPRA_VP               '+
                                            '      ,DTA_MAIOR_COMPRA_VP             '+
                                            '      ,VLR_MAIOR_COMPRA_VP             '+
                                            '      ,QTD_COMPRAS_VP                  '+
                                            '      ,QTD_MAIOR_ATRASO                '+
                                            '      ,DTA_MAIOR_ATRASO                '+
                                            '      ,NUM_MDA                         '+
                                            '      ,VLR_SALDO                       '+
                                            '      ,QTD_COBRANCA                    '+
                                            '      ,QTD_NEGATIVACAO                 '+
                                            '      ,QTD_CHEQUE_DEVOL                '+
                                            '      ,IND_SPC                         '+
                                            '      ,DTA_NEGATIVACAO                 '+
                                            '      ,DTA_REABILITACAO                '+
                                            '      ,DTA_PRESTACAO_NEG               '+
                                            '      ,VLR_PRESTACAO_NEG               '+
                                            '      ,NUM_PARCELA_NEG                 '+
                                            '      ,COD_BLOQUEIO                    '+
                                            '      ,DTA_BLOQUEIO                    '+
                                            '      ,DTA_ATUALIZACAO                 '+
                                            '      ,DTA_SISTEMA                     '+
                                            '      ,IND_STATUS                      '+
                                            '      ,VLR_NEGATIVADO                  '+
                                            '      ,COD_CONTRATO                    '+
                                            '      ,QTD_ULT_ATRASO                  '+
                                            '      ,DTA_ULT_ATRASO                  '+
                                            '      ,NUM_MDA_ANT                     '+
                                            '      ,DTA_MVTO_SPC                    '+
                                            '      ,VLR_SALDO_CPP                   '+
                                            '      ,COD_PERFIL_CLI                  '+
                                            '      ,CRED_REDES                      '+
                                            '      ,QTD_COMPRAS_CP                  '+
                                            '      ,MAIOR_ACUM                      '+
                                            '      ,DTA_MAIOR_ACUM                  '+
                                            '      ,DTA_WS_CONV                     '+
                                            '      ,HORA_WS_CONV                    '+
                                            '      ,IND_WS_CONV                     '+
                                            ' from CRE_SALDOS_CLI where DTA_ATUALIZACAO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_SALDOS_CLI.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_SALDOS_CLI';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_SALDOS_CLI ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue=
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from EST_CUPONS_CANCELADOS  where DTA_MOVIMENTO >= current_date -7 ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CUPONS_CANCELADOS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_CANC';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
        }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS_CANCELADOS  where DTA_MOVIMENTO >= current_date -7 ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_CANC';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from EST_CUPONS_CANCELADOS  where DTA_MOVIMENTO >= current_date -7 ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_CANC';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CUPONS_CANC ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' +
        ' from EST_CUPONS_DEVOLUCAO  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CUPONS_DEVOLUCAO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_DEVOLUCAO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from EST_CUPONS_DEVOLUCAO  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_DEVOLUCAO';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from EST_CUPONS_DEVOLUCAO  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_DEVOLUCAO';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CUPONS_DEVOLUCAO ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :='   select COD_EMP                                 '+
                                        '         ,COD_UNIDADE                             '+
                                        '         ,NUM_EQUIPAMENTO                         '+
                                        '         ,NUM_CUPOM                               '+
                                        '         ,DES_CHAVE_NFCE                          '+
                                        '         ,DES_NOME_XML                            '+
                                        '         ,COD_RESULTADO                           '+
                                        '         ,DES_RESULTADO                           '+
                                        '         ,TIP_AMBIENTE                            '+
                                        '         ,VERSAO_NFCE                             '+
                                        '         ,PROTOCOLO                               '+
                                        '         ,RECIBO                                  '+
                                        '         ,IND_CONTINGENCIA                        '+
                                        '         ,DTA_MOVIMENTO                           '+
                                        '         ,HORA_AUTORIZACAO                        '+
                                        '         ,LINK_CONSULTA_NFCE                      '+
                                        '         ,IND_CANCELADO                           '+
                                        '         ,IP_ENVIO                                '+
                                        '         ,XML                                     '+
                                        '    FROM EST_CUPONS_NFCE                           '+
                                        '   WHERE DTA_MOVIMENTO >= CURRENT_DATE - '+(sDiasXMLLoja)+
                                       // '     AND cod_resultado in (0,101,150,100) '+
                                        '   order by                                       '+
                                        '   COD_EMP,                                       '+
                                        '   COD_UNIDADE,                                   '+
                                        '   NUM_EQUIPAMENTO,                               '+
                                        '   NUM_CUPOM                                      ';


      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CUPONS_NFCE.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_NFCE_XML';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;   }
      {FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
       }
      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=  '   select COD_EMP                                 '+
                                        '         ,COD_UNIDADE                             '+
                                        '         ,NUM_EQUIPAMENTO                         '+
                                        '         ,NUM_CUPOM                               '+
                                        '         ,DES_CHAVE_NFCE                          '+
                                        '         ,DES_NOME_XML                            '+
                                        '         ,COD_RESULTADO                           '+
                                        '         ,DES_RESULTADO                           '+
                                        '         ,TIP_AMBIENTE                            '+
                                        '         ,VERSAO_NFCE                             '+
                                        '         ,PROTOCOLO                               '+
                                        '         ,RECIBO                                  '+
                                        '         ,IND_CONTINGENCIA                        '+
                                        '         ,DTA_MOVIMENTO                           '+
                                        '         ,HORA_AUTORIZACAO                        '+
                                        '         ,LINK_CONSULTA_NFCE                      '+
                                        '         ,IND_CANCELADO                           '+
                                        '         ,IP_ENVIO                                '+
                                        '         ,XML                                     '+
                                        '    FROM EST_CUPONS_NFCE                           '+
                                        '   WHERE DTA_MOVIMENTO >= CURRENT_DATE - '+(sDiasXMLLoja)+
                                        '     AND cod_resultado in (101,150,100) '+
                                        '   order by                                       '+
                                        '   COD_EMP,                                       '+
                                        '   COD_UNIDADE,                                   '+
                                        '   NUM_EQUIPAMENTO,                               '+
                                        '   NUM_CUPOM                                      ';

      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_NFCE_XML';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=  '   select COD_EMP                                 '+
                                        '         ,COD_UNIDADE                             '+
                                        '         ,NUM_EQUIPAMENTO                         '+
                                        '         ,NUM_CUPOM                               '+
                                        '         ,DES_CHAVE_NFCE                          '+
                                        '         ,DES_NOME_XML                            '+
                                        '         ,COD_RESULTADO                           '+
                                        '         ,DES_RESULTADO                           '+
                                        '         ,TIP_AMBIENTE                            '+
                                        '         ,VERSAO_NFCE                             '+
                                        '         ,PROTOCOLO                               '+
                                        '         ,RECIBO                                  '+
                                        '         ,IND_CONTINGENCIA                        '+
                                        '         ,DTA_MOVIMENTO                           '+
                                        '         ,HORA_AUTORIZACAO                        '+
                                        '         ,LINK_CONSULTA_NFCE                      '+
                                        '         ,IND_CANCELADO                           '+
                                        '         ,IP_ENVIO                                '+
                                        '         ,XML                                     '+
                                        '   from EST_CUPONS_NFCE                           '+
                                        '   WHERE DTA_MOVIMENTO >= CURRENT_DATE - '+(sDiasXMLLoja)+
                                        '   order by                                       '+
                                        '   COD_EMP,                                       '+
                                        '   COD_UNIDADE,                                   '+
                                        '   NUM_EQUIPAMENTO,                               '+
                                        '   NUM_CUPOM                                      ';
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_NFCE_XML';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CUPONS_NFCE_XML ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=' select COD_EMP              ' +
        '       ,COD_UNIDADE          ' + '       ,NUM_EQUIPAMENTO      ' +
        '       ,NUM_CUPOM            ' + '       ,DES_CHAVE_NFCE       ' +
        '       ,DES_NOME_XML         ' + '       ,COD_RESULTADO        ' +
        '       ,TIP_AMBIENTE         ' + '       ,VERSAO_NFCE          ' +
        '       ,PROTOCOLO            ' + '       ,RECIBO               ' +
        '       ,IND_CONTINGENCIA     ' + '       ,DTA_MOVIMENTO        ' +
        '       ,HORA_AUTORIZACAO     ' +
      // '       ,LINK_CONSULTA_NFCE   '+
        '       ,IND_CANCELADO        ' + '       ,IND_ATUALIZAR        ' +
        '       ,IP_ENVIO             ' +
        ' from EST_CUPONS_NFCE  where DTA_MOVIMENTO >= current_date - ' +
        (sDiasProtocolo);

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CUPONS_NFCE_LJ.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_NFCE_LJ';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP              ' +
        '       ,COD_UNIDADE          ' + '       ,NUM_EQUIPAMENTO      ' +
        '       ,NUM_CUPOM            ' + '       ,DES_CHAVE_NFCE       ' +
        '       ,DES_NOME_XML         ' + '       ,COD_RESULTADO        ' +
        '       ,TIP_AMBIENTE         ' + '       ,VERSAO_NFCE          ' +
        '       ,PROTOCOLO            ' + '       ,RECIBO               ' +
        '       ,IND_CONTINGENCIA     ' + '       ,DTA_MOVIMENTO        ' +
        '       ,HORA_AUTORIZACAO     ' +
      // '       ,LINK_CONSULTA_NFCE   '+
        '       ,IND_CANCELADO        ' + '       ,IND_ATUALIZAR        ' +
        '       ,IP_ENVIO             ' +
        ' from EST_CUPONS_NFCE  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_NFCE_LJ';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP              ' +
        '       ,COD_UNIDADE          ' + '       ,NUM_EQUIPAMENTO      ' +
        '       ,NUM_CUPOM            ' + '       ,DES_CHAVE_NFCE       ' +
        '       ,DES_NOME_XML         ' + '       ,COD_RESULTADO        ' +
        '       ,TIP_AMBIENTE         ' + '       ,VERSAO_NFCE          ' +
        '       ,PROTOCOLO            ' + '       ,RECIBO               ' +
        '       ,IND_CONTINGENCIA     ' + '       ,DTA_MOVIMENTO        ' +
        '       ,HORA_AUTORIZACAO     ' +
      // '       ,LINK_CONSULTA_NFCE   '+
        '       ,IND_CANCELADO        ' + '       ,IND_ATUALIZAR        ' +
        '       ,IP_ENVIO             ' +
        ' from EST_CUPONS_NFCE  where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_NFCE_LJ';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CUPONS_NFCE_LJ ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=' select *  ' +
        ' from GER_LIBERACOES  where DTA_LIBERACAO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_LIBERACOES.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_LIBERACOES';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end; }
     {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from GER_LIBERACOES  where DTA_LIBERACAO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_LIBERACOES';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from GER_LIBERACOES  where DTA_LIBERACAO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_LIBERACOES';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_LIBERACOES ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=' select *  ' +
        ' from GER_MVTO_ACESSOS  where DTA_MVTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_MVTO_ACESSOS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_MVTO_ACESSOS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
       }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from GER_MVTO_ACESSOS  where DTA_MVTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_MVTO_ACESSOS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from GER_MVTO_ACESSOS  where DTA_MVTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_MVTO_ACESSOS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_MVTO_ACESSOS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=' select COD_EMP                                  '+
                                        '   ,COD_UNIDADE                              '+
                                        '   ,DTA_MVTO                                 '+
                                        '   ,IND_SIT_CAIXA                            '+
                                        '   ,VLR_SALDO_INICIAL                        '+
                                        '   ,VLR_DEBITO                               '+
                                        '   ,VLR_CREDITO                              '+
                                        '   ,VLR_NUMERARIO                            '+
                                        '   ,VLR_NUMERARIO_ORIG                       '+
                                        '   ,VLR_CHEQUES                              '+
                                        '   ,VLR_CHEQUES_ORIG                         '+
                                        '   ,VLR_DESP_LANCAR                          '+
                                        '   ,VLR_DEP_NUMERARIO                        '+
                                        '   ,VLR_DEP_CHEQUES                          '+
                                        '   ,DTA_SISTEMA                              '+
                                        '   ,0 as IND_STATUS                          '+
                                        '   ,DES_HORA_FECHAMENTO                      '+
                                        '   ,DTA_FECHAMENTO                           '+
                                        '   ,COD_USUARIO                              '+
                                        '   ,current_date as DATA_ULT_ALTERACAO       '+
                                        '   ,0 as REP_STATUS                          '+
                                        '   ,VLR_TROCO                                '+
                                        '   ,JUST_VLR_EXCEDIDO                        '+
                                        '   ,OBS_VLR_EXCEDIDO                         '+
                                        '   ,JUST_DIF_CAIXA                           '+
                                            ' from CXS_SALDO_CAIXAS  where DTA_MVTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_SALDO_CAIXAS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_SALDO_CAIXAS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
       }
      {DtmPGConnection.FDConnection1.Connected := True;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=  ' select COD_EMP                                  '+
                                        '   ,COD_UNIDADE                              '+
                                        '   ,DTA_MVTO                                 '+
                                        '   ,IND_SIT_CAIXA                            '+
                                        '   ,VLR_SALDO_INICIAL                        '+
                                        '   ,VLR_DEBITO                               '+
                                        '   ,VLR_CREDITO                              '+
                                        '   ,VLR_NUMERARIO                            '+
                                        '   ,VLR_NUMERARIO_ORIG                       '+
                                        '   ,VLR_CHEQUES                              '+
                                        '   ,VLR_CHEQUES_ORIG                         '+
                                        '   ,VLR_DESP_LANCAR                          '+
                                        '   ,VLR_DEP_NUMERARIO                        '+
                                        '   ,VLR_DEP_CHEQUES                          '+
                                        '   ,DTA_SISTEMA                              '+
                                        '   ,0 as IND_STATUS                          '+
                                        '   ,DES_HORA_FECHAMENTO                      '+
                                        '   ,DTA_FECHAMENTO                           '+
                                        '   ,COD_USUARIO                              '+
                                        '   ,current_date as DATA_ULT_ALTERACAO       '+
                                        '   ,0 as REP_STATUS                          '+
                                        '   ,VLR_TROCO                                '+
                                        '   ,JUST_VLR_EXCEDIDO                        '+
                                        '   ,OBS_VLR_EXCEDIDO                         '+
                                        '   ,JUST_DIF_CAIXA                           '+
                                            ' from CXS_SALDO_CAIXAS  where DTA_MVTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_SALDO_CAIXAS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=  ' select COD_EMP                         '+
                                        '   ,COD_UNIDADE                         '+
                                        '   ,DTA_MVTO                            '+
                                        '   ,COD_OPER_CAIXA                      '+
                                        '   ,NUM_SEQ_LCTO                        '+
                                        '   ,COD_CONTA_CB                        '+
                                        '   ,COD_UNIDADE_CB                      '+
                                        '   ,COD_CC_CB                           '+
                                        '   ,VLR_DEBITO                          '+
                                        '   ,VLR_DEBITO_ORIG                     '+
                                        '   ,VLR_CREDITO                         '+
                                        '   ,VLR_CREDITO_ORIG                    '+
                                        '   ,COD_HIST_CB                         '+
                                        '   ,DES_HIST1                           '+
                                        '   ,DES_HIST2                           '+
                                        '   ,DES_HIST3                           '+
                                        '   ,DES_HIST4                           '+
                                        '   ,COD_ORIGEM_MVTO                     '+
                                        '   ,DTA_SISTEMA                         '+
                                        '   ,IND_STATUS                          '+
                                        '   ,COD_USUARIO                         '+
                                        '   ,DES_HORA_GERACAO                    '+
                                        '   ,CURRENT_DATE AS DATA_ULT_ALTERACAO  '+
                                        '   ,0 AS REP_STATUS                     '+
                                        '   ,DES_DOCUMENTO                       '+
                                        '   ,NUM_SEQ_DESPESA                     '+
                                        '   ,IND_EXCLUSAO                        '+
                                        '   ,NUM_MIN_TOLERANCIA                  '+
                                        '   ,DES_ACERTO                          '+
                                        ' from CXS_MVTO_CAIXAS where DTA_MVTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_SALDO_CAIXAS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add
            ('Replicou Tabela CXS_SALDO_CAIXAS(mig_stg_cxs_saldos_caixas) ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=' select *  ' +
        ' from CXS_MVTO_DESPESAS  where DTA_MVTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_MVTO_DESPESAS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_DESPESAS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from CXS_MVTO_DESPESAS  where DTA_MVTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_DESPESAS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from CXS_MVTO_DESPESAS  where DTA_MVTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_DESPESAS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add
            ('Replicou Tabela CXS_MVTO_DESPESAS(mig_stg_cxs_mvto_despesas) ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text :=' select *  ' +
        ' from CXS_MVTO_CAIXA_DESP  where DTA_BOLETIM >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_MVTO_CAIXA_DESP.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_CAIXA_DESP';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from CXS_MVTO_CAIXA_DESP  where DTA_BOLETIM >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_CAIXA_DESP';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from CXS_MVTO_CAIXA_DESP  where DTA_BOLETIM >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_CAIXA_DESP';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;}
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add
            ('Replicou Tabela CXS_MVTO_CAIXA_DESP(mig_stg_cxs_mvto_cx_desp) ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select COD_EMP                             '+
                                        '       ,COD_UNIDADE                         '+
                                        '       ,DTA_MVTO                            '+
                                        '       ,COD_OPER_CAIXA                      '+
                                        '       ,NUM_SEQ_LCTO                        '+
                                        '       ,COD_CONTA_CB                        '+
                                        '       ,COD_UNIDADE_CB                      '+
                                        '       ,COD_CC_CB                           '+
                                        '       ,VLR_DEBITO                          '+
                                        '       ,VLR_DEBITO_ORIG                     '+
                                        '       ,VLR_CREDITO                         '+
                                        '       ,VLR_CREDITO_ORIG                    '+
                                        '       ,COD_HIST_CB                         '+
                                        '       ,DES_HIST1                           '+
                                        '       ,DES_HIST2                           '+
                                        '       ,DES_HIST3                           '+
                                        '       ,DES_HIST4                           '+
                                        '       ,COD_ORIGEM_MVTO                     '+
                                        '       ,DTA_SISTEMA                         '+
                                        '       ,IND_STATUS                          '+
                                        '       ,COD_USUARIO                         '+
                                        '       ,DES_HORA_GERACAO                    '+
                                        '       ,CURRENT_DATE AS DATA_ULT_ALTERACAO  '+
                                        '       ,0 AS REP_STATUS                     '+
                                        '       ,DES_DOCUMENTO                       '+
                                        '       ,NUM_SEQ_DESPESA                     '+
                                        '       ,IND_EXCLUSAO                        '+
                                        '       ,NUM_MIN_TOLERANCIA                  '+
                                        '       ,DES_ACERTO                          '+
                                        ' from CXS_MVTO_CAIXAS  where DTA_MVTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_MVTO_CAIXAS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_CAIXAS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end; }
      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=  ' select COD_EMP                             '+
                                        '       ,COD_UNIDADE                         '+
                                        '       ,DTA_MVTO                            '+
                                        '       ,COD_OPER_CAIXA                      '+
                                        '       ,NUM_SEQ_LCTO                        '+
                                        '       ,COD_CONTA_CB                        '+
                                        '       ,COD_UNIDADE_CB                      '+
                                        '       ,COD_CC_CB                           '+
                                        '       ,VLR_DEBITO                          '+
                                        '       ,VLR_DEBITO_ORIG                     '+
                                        '       ,VLR_CREDITO                         '+
                                        '       ,VLR_CREDITO_ORIG                    '+
                                        '       ,COD_HIST_CB                         '+
                                        '       ,DES_HIST1                           '+
                                        '       ,DES_HIST2                           '+
                                        '       ,DES_HIST3                           '+
                                        '       ,DES_HIST4                           '+
                                        '       ,COD_ORIGEM_MVTO                     '+
                                        '       ,DTA_SISTEMA                         '+
                                        '       ,IND_STATUS                          '+
                                        '       ,COD_USUARIO                         '+
                                        '       ,DES_HORA_GERACAO                    '+
                                        '       ,CURRENT_DATE AS DATA_ULT_ALTERACAO  '+
                                        '       ,0 AS REP_STATUS                     '+
                                        '       ,DES_DOCUMENTO                       '+
                                        '       ,NUM_SEQ_DESPESA                     '+
                                        '       ,IND_EXCLUSAO                        '+
                                        '       ,NUM_MIN_TOLERANCIA                  '+
                                        '       ,DES_ACERTO                          '+
                                        ' from CXS_MVTO_CAIXAS  where DTA_MVTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_CAIXAS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=  ' select COD_EMP                             '+
                                        '       ,COD_UNIDADE                         '+
                                        '       ,DTA_MVTO                            '+
                                        '       ,COD_OPER_CAIXA                      '+
                                        '       ,NUM_SEQ_LCTO                        '+
                                        '       ,COD_CONTA_CB                        '+
                                        '       ,COD_UNIDADE_CB                      '+
                                        '       ,COD_CC_CB                           '+
                                        '       ,VLR_DEBITO                          '+
                                        '       ,VLR_DEBITO_ORIG                     '+
                                        '       ,VLR_CREDITO                         '+
                                        '       ,VLR_CREDITO_ORIG                    '+
                                        '       ,COD_HIST_CB                         '+
                                        '       ,DES_HIST1                           '+
                                        '       ,DES_HIST2                           '+
                                        '       ,DES_HIST3                           '+
                                        '       ,DES_HIST4                           '+
                                        '       ,COD_ORIGEM_MVTO                     '+
                                        '       ,DTA_SISTEMA                         '+
                                        '       ,IND_STATUS                          '+
                                        '       ,COD_USUARIO                         '+
                                        '       ,DES_HORA_GERACAO                    '+
                                        '       ,CURRENT_DATE AS DATA_ULT_ALTERACAO  '+
                                        '       ,0 AS REP_STATUS                     '+
                                        '       ,DES_DOCUMENTO                       '+
                                        '       ,NUM_SEQ_DESPESA                     '+
                                        '       ,IND_EXCLUSAO                        '+
                                        '       ,NUM_MIN_TOLERANCIA                  '+
                                        '       ,DES_ACERTO                          '+
                                        ' from CXS_MVTO_CAIXAS   where DTA_MVTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_MVTO_CAIXAS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CXS_MVTO_CAIXAS(mig_stg_cxs_mvto_caixas) '
            + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
       DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select  CXS_ECF_REDUCAOZ.COD_EMP            ' +
        '  ,CXS_ECF_REDUCAOZ.COD_UNIDADE              ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_EQUIPAMENTO          ' +
        '  ,CXS_ECF_REDUCAOZ.DTA_MOVIMENTO            ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_REDUCAOZ             ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_MAPA_RESUMO          ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_CUPOM                ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_CUPOM_FISCAL         ' +
        '  ,CXS_ECF_REDUCAOZ.DES_MODELO_ECF           ' +
        '  ,CXS_ECF_REDUCAOZ.DES_SERIE_ECF            ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_LEITURAX             ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REDUCAOZ             ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REDUCAOZ_RESTANTES   ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REINICIO_OP          ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_GT_INICIAL           ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_GT_FINAL             ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_VENDA_BRUTA          ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_CUPONS_CANCELADOS    ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_CUPONS_CANCELADOS    ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ITENS_CANCELADOS     ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_DESCONTOS            ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_VENDA_LIQUIDA        ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ISENTO               ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_NAO_TRIBUTADO        ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_SUBST_TRIBUTARIA     ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_TOTAL_ISSQN          ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_ACRESCIMO            ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ACRESCIMO            ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_ACRESCIMO_SERV       ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ACRESCIMO_SERV       ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_CHEQUES_EMITIDOS     ' +
        '  ,CXS_ECF_REDUCAOZ.DTA_SISTEMA              ' +
        '  ,CXS_ECF_REDUCAOZ.TEMPO_OPERACIONAL        ' +
        '  ,CXS_ECF_REDUCAOZ.TEMPO_IMPRESSAO          ' +
        '  ,CXS_ECF_REDUCAOZ.MF_ADIC                  ' +
        '  ,CXS_ECF_REDUCAOZ.COD_USUARIO_ECF          ' +
        '  ,CXS_ECF_REDUCAOZ.HORA_EMISSAO             ' +
        '  ,CXS_ECF_REDUCAOZ.GNF                      ' +
        '  ,CXS_ECF_REDUCAOZ.IND_NFCE                 ' +
        '  from CXS_ECF_REDUCAOZ                      ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_ECF_REDUCAOZ.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
         }
       {
      FDBatchMoveSQLReader1.ReadSQL :=
        ' select  CXS_ECF_REDUCAOZ.COD_EMP            ' +
        '  ,CXS_ECF_REDUCAOZ.COD_UNIDADE              ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_EQUIPAMENTO          ' +
        '  ,CXS_ECF_REDUCAOZ.DTA_MOVIMENTO            ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_REDUCAOZ             ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_MAPA_RESUMO          ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_CUPOM                ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_CUPOM_FISCAL         ' +
        '  ,CXS_ECF_REDUCAOZ.DES_MODELO_ECF           ' +
        '  ,CXS_ECF_REDUCAOZ.DES_SERIE_ECF            ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_LEITURAX             ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REDUCAOZ             ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REDUCAOZ_RESTANTES   ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REINICIO_OP          ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_GT_INICIAL           ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_GT_FINAL             ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_VENDA_BRUTA          ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_CUPONS_CANCELADOS    ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_CUPONS_CANCELADOS    ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ITENS_CANCELADOS     ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_DESCONTOS            ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_VENDA_LIQUIDA        ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ISENTO               ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_NAO_TRIBUTADO        ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_SUBST_TRIBUTARIA     ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_TOTAL_ISSQN          ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_ACRESCIMO            ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ACRESCIMO            ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_ACRESCIMO_SERV       ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ACRESCIMO_SERV       ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_CHEQUES_EMITIDOS     ' +
        '  ,CXS_ECF_REDUCAOZ.DTA_SISTEMA              ' +
        '  ,CXS_ECF_REDUCAOZ.TEMPO_OPERACIONAL        ' +
        '  ,CXS_ECF_REDUCAOZ.TEMPO_IMPRESSAO          ' +
        '  ,CXS_ECF_REDUCAOZ.MF_ADIC                  ' +
        '  ,CXS_ECF_REDUCAOZ.COD_USUARIO_ECF          ' +
        '  ,CXS_ECF_REDUCAOZ.HORA_EMISSAO             ' +
        '  ,CXS_ECF_REDUCAOZ.GNF                      ' +
        '  ,CXS_ECF_REDUCAOZ.IND_NFCE                 ' +
        '  from CXS_ECF_REDUCAOZ                      ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        ' select  CXS_ECF_REDUCAOZ.COD_EMP            ' +
        '  ,CXS_ECF_REDUCAOZ.COD_UNIDADE              ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_EQUIPAMENTO          ' +
        '  ,CXS_ECF_REDUCAOZ.DTA_MOVIMENTO            ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_REDUCAOZ             ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_MAPA_RESUMO          ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_CUPOM                ' +
        '  ,CXS_ECF_REDUCAOZ.NUM_CUPOM_FISCAL         ' +
        '  ,CXS_ECF_REDUCAOZ.DES_MODELO_ECF           ' +
        '  ,CXS_ECF_REDUCAOZ.DES_SERIE_ECF            ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_LEITURAX             ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REDUCAOZ             ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REDUCAOZ_RESTANTES   ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_REINICIO_OP          ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_GT_INICIAL           ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_GT_FINAL             ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_VENDA_BRUTA          ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_CUPONS_CANCELADOS    ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_CUPONS_CANCELADOS    ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ITENS_CANCELADOS     ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_DESCONTOS            ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_VENDA_LIQUIDA        ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ISENTO               ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_NAO_TRIBUTADO        ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_SUBST_TRIBUTARIA     ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_TOTAL_ISSQN          ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_ACRESCIMO            ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ACRESCIMO            ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_ACRESCIMO_SERV       ' +
        '  ,CXS_ECF_REDUCAOZ.VLR_ACRESCIMO_SERV       ' +
        '  ,CXS_ECF_REDUCAOZ.QTD_CHEQUES_EMITIDOS     ' +
        '  ,CXS_ECF_REDUCAOZ.DTA_SISTEMA              ' +
        '  ,CXS_ECF_REDUCAOZ.TEMPO_OPERACIONAL        ' +
        '  ,CXS_ECF_REDUCAOZ.TEMPO_IMPRESSAO          ' +
        '  ,CXS_ECF_REDUCAOZ.MF_ADIC                  ' +
        '  ,CXS_ECF_REDUCAOZ.COD_USUARIO_ECF          ' +
        '  ,CXS_ECF_REDUCAOZ.HORA_EMISSAO             ' +
        '  ,CXS_ECF_REDUCAOZ.GNF                      ' +
        '  ,CXS_ECF_REDUCAOZ.IND_NFCE                 ' +
        '  from CXS_ECF_REDUCAOZ                      ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CXS_ECF_REDUCAOZ  ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '    select CXS_ECF_REDUCAOZ_FP.COD_EMP    ' +
        '          ,CXS_ECF_REDUCAOZ_FP.COD_UNIDADE        ' +
        '          ,CXS_ECF_REDUCAOZ_FP.NUM_EQUIPAMENTO    ' +
        '          ,CXS_ECF_REDUCAOZ_FP.DTA_MOVIMENTO      ' +
        '          ,CXS_ECF_REDUCAOZ_FP.NUM_REDUCAOZ       ' +
        '          ,CXS_ECF_REDUCAOZ_FP.COD_FINALIZADORA   ' +
        '          ,CXS_ECF_REDUCAOZ_FP.VLR_FINALIZADORA   ' +
        '     from CXS_ECF_REDUCAOZ_FP                      ' +
        '    where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_ECF_REDUCAOZ_FP.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_FP';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end; }

      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '    select CXS_ECF_REDUCAOZ_FP.COD_EMP    ' +
        '          ,CXS_ECF_REDUCAOZ_FP.COD_UNIDADE        ' +
        '          ,CXS_ECF_REDUCAOZ_FP.NUM_EQUIPAMENTO    ' +
        '          ,CXS_ECF_REDUCAOZ_FP.DTA_MOVIMENTO      ' +
        '          ,CXS_ECF_REDUCAOZ_FP.NUM_REDUCAOZ       ' +
        '          ,CXS_ECF_REDUCAOZ_FP.COD_FINALIZADORA   ' +
        '          ,CXS_ECF_REDUCAOZ_FP.VLR_FINALIZADORA   ' +
        '     from CXS_ECF_REDUCAOZ_FP                      ' +
        '    where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_FP';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '    select CXS_ECF_REDUCAOZ_FP.COD_EMP    ' +
        '          ,CXS_ECF_REDUCAOZ_FP.COD_UNIDADE        ' +
        '          ,CXS_ECF_REDUCAOZ_FP.NUM_EQUIPAMENTO    ' +
        '          ,CXS_ECF_REDUCAOZ_FP.DTA_MOVIMENTO      ' +
        '          ,CXS_ECF_REDUCAOZ_FP.NUM_REDUCAOZ       ' +
        '          ,CXS_ECF_REDUCAOZ_FP.COD_FINALIZADORA   ' +
        '          ,CXS_ECF_REDUCAOZ_FP.VLR_FINALIZADORA   ' +
        '     from CXS_ECF_REDUCAOZ_FP                      ' +
        '    where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_FP';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add
            ('Replicou Tabela CXS_ECF_REDUCAOZ_FP_LJ (mig_stg_cxs_reducaoz_fp) '
            + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '  select CXS_ECF_REDUCAOZ_ICMS.COD_EMP           ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.COD_UNIDADE       ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.NUM_EQUIPAMENTO   ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.DTA_MOVIMENTO     ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.NUM_REDUCAOZ      ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.PER_ICMS          ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.VLR_BC_ICMS       ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.VLR_ICMS          ' +
        '  from CXS_ECF_REDUCAOZ_ICMS                     ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_ECF_REDUCAOZ_ICMS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_ICMS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  select CXS_ECF_REDUCAOZ_ICMS.COD_EMP           ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.COD_UNIDADE       ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.NUM_EQUIPAMENTO   ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.DTA_MOVIMENTO     ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.NUM_REDUCAOZ      ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.PER_ICMS          ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.VLR_BC_ICMS       ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.VLR_ICMS          ' +
        '  from CXS_ECF_REDUCAOZ_ICMS                     ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_ICMS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  select CXS_ECF_REDUCAOZ_ICMS.COD_EMP           ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.COD_UNIDADE       ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.NUM_EQUIPAMENTO   ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.DTA_MOVIMENTO     ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.NUM_REDUCAOZ      ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.PER_ICMS          ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.VLR_BC_ICMS       ' +
        '        ,CXS_ECF_REDUCAOZ_ICMS.VLR_ICMS          ' +
        '  from CXS_ECF_REDUCAOZ_ICMS                     ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_ICMS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add
            ('Replicou Tabela CXS_ECF_REDUCAOZ_ICMS_LJ (mig_stg_cxs_reducaoz_icms) '
            + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '  select CXS_ECF_REDUCAOZ_LNFISCAL.COD_EMP  ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.COD_UNIDADE      ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.NUM_EQUIPAMENTO  ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.DTA_MOVIMENTO    ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.NUM_REDUCAOZ     ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.COD_LNFISCAL     ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.VLR_LNFISCAL     ' +
        '  from CXS_ECF_REDUCAOZ_LNFISCAL                   ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_ECF_REDUCAOZ_LNFISCAL.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_LNFISCAL';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }

      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  select CXS_ECF_REDUCAOZ_LNFISCAL.COD_EMP  ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.COD_UNIDADE      ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.NUM_EQUIPAMENTO  ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.DTA_MOVIMENTO    ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.NUM_REDUCAOZ     ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.COD_LNFISCAL     ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.VLR_LNFISCAL     ' +
        '  from CXS_ECF_REDUCAOZ_LNFISCAL                   ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_LNFISCAL';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  select CXS_ECF_REDUCAOZ_LNFISCAL.COD_EMP  ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.COD_UNIDADE      ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.NUM_EQUIPAMENTO  ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.DTA_MOVIMENTO    ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.NUM_REDUCAOZ     ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.COD_LNFISCAL     ' +
        '        ,CXS_ECF_REDUCAOZ_LNFISCAL.VLR_LNFISCAL     ' +
        '  from CXS_ECF_REDUCAOZ_LNFISCAL                   ' +
        '  where dta_movimento >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_REDUCAOZ_LNFISCAL';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add
            ('Replicou Tabela CXS_ECF_REDUCAOZ_LNFISCAL_LJ (mig_stg_cxs_reducaoz_lnf) '
            + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' + ' from CXS_ECF_CHEQUES '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_ECF_CHEQUES.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_CHEQUES';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CXS_ECF_CHEQUES '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_CHEQUES';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CXS_ECF_CHEQUES '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_CHEQUES';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }

      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CXS_ECF_CHEQUES ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' + ' from CXS_ECF_CONTROLE '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_ECF_CONTROLE.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_CONTROLE';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CXS_ECF_CONTROLE '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_CONTROLE';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CXS_ECF_CONTROLE '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_CONTROLE';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CXS_ECF_CONTROLE ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' + ' from CXS_ECF_MVTO ' +
        ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_ECF_MVTO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_MVTO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      }
      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CXS_ECF_MVTO ' +
        ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_MVTO';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CXS_ECF_MVTO ' +
        ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_MVTO';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CXS_ECF_MVTO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' + ' from CXS_ECF_MVTO_FP '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CXS_ECF_MVTO_FP.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_MVTO_FP';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CXS_ECF_MVTO_FP '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_MVTO_FP';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CXS_ECF_MVTO_FP '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CXS_ECF_MVTO_FP';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CXS_ECF_MVTO_FP ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
       DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '    select COD_EMP                   ' +
        '          ,' + IntToStr(FrmPrincipal.iCod_unidade) +
        ' AS  COD_UNIDADE_FB ' + '          ,COD_CLIENTE               ' +
        '          ,DES_CLIENTE               ' +
        '          ,DES_FANTASIA              ' +
        '          ,COD_PESSOA                ' +
        '          ,COD_UNIDADE               ' +
        '          ,DES_ENDERECO              ' +
        '          ,DES_PTO_REFER             ' +
        '          ,DES_BAIRRO                ' +
        '          ,NUM_CEP                   ' +
        '          ,COD_CIDADE                ' +
        '          ,NUM_CAIXA_POSTAL          ' +
        '          ,TIP_ENDERECO              ' +
        '          ,IND_MALA_DIRETA           ' +
        '          ,IND_CORREIO               ' +
        '          ,DES_EMAIL                 ' +
        '          ,TIP_PESSOA                ' +
        '          ,DTA_NASCTO                ' +
        '          ,COD_PROFISSAO             ' +
        '          ,TIP_SEXO                  ' +
        '          ,NUM_CPF_CNPJ              ' +
        '          ,NUM_INSCRICAO             ' +
        '          ,TIP_CLIENTE               ' +
        '          ,DTA_CADASTRO              ' +
        '          ,DTA_ALTERACAO             ' +
        '          ,DTA_SISTEMA               ' +
        '          ,IND_INATIVO               ' +
        '          ,DES_TELEFONE              ' +
        '          ,DES_FONE_CELULAR          ' +
        '          ,DES_RUA_END               ' +
        '          ,DES_NUM_END               ' +
        '          ,DES_COMPL_END             ' +
        '          ,IND_SIT_CADASTRO          ' +
        '          ,COD_SITUACAO              ' +
        '          ,DTA_ATUALIZACAO_CAD       ' +
        '          ,IND_NOVO_LAYOUT           ' +
        '          ,NUM_EQUIPAMENTO           ' +
        '          ,HORA_CADASTRO             ' +
        '          ,NUM_USUARIO               ' +
        '          ,HORA_ALTERACAO            ' +
        '          ,COD_AUTORIZANTE_ATRASO    ' +
        '          ,DES_CARTAO                ' +
        '          ,COD_OBS                   ' +
        '          ,FONE_ULT_CONTATADO        ' +
        '          ,MELHOR_HORA_CONTATO       ' +
        '          ,COD_EMP_CADASTRO          ' +
        '          ,COD_CARTAO                ' +
        '          ,IND_CLI_UNIFICADO         ' +
        '          ,IND_ATUALIZA_ADM          ' +
        '    from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and  Tip_Cliente in (1,2,3,4)  ' +
        '    and ((Dta_Cadastro >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text) +
        '    and Dta_Cadastro <= current_date   ' + '    or (Dta_Alteracao >= current_date  - ' +(FrmPrincipal.edtDiasReplicacao.text) +
        '    and Dta_Alteracao <= current_date))) ' +
        '    order by  COD_EMP,  COD_CLIENTE  ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_CLIENTES.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
        }
       {
      DtmPGConnection.FDConnection1.Connected := True;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := '    select COD_EMP                   ' +
        '          ,' + IntToStr(FrmPrincipal.iCod_unidade) +
        ' AS  COD_UNIDADE_FB ' + '          ,COD_CLIENTE               ' +
        '          ,DES_CLIENTE               ' +
        '          ,DES_FANTASIA              ' +
        '          ,COD_PESSOA                ' +
        '          ,COD_UNIDADE               ' +
        '          ,DES_ENDERECO              ' +
        '          ,DES_PTO_REFER             ' +
        '          ,DES_BAIRRO                ' +
        '          ,NUM_CEP                   ' +
        '          ,COD_CIDADE                ' +
        '          ,NUM_CAIXA_POSTAL          ' +
        '          ,TIP_ENDERECO              ' +
        '          ,IND_MALA_DIRETA           ' +
        '          ,IND_CORREIO               ' +
        '          ,DES_EMAIL                 ' +
        '          ,TIP_PESSOA                ' +
        '          ,DTA_NASCTO                ' +
        '          ,COD_PROFISSAO             ' +
        '          ,TIP_SEXO                  ' +
        '          ,NUM_CPF_CNPJ              ' +
        '          ,NUM_INSCRICAO             ' +
        '          ,TIP_CLIENTE               ' +
        '          ,DTA_CADASTRO              ' +
        '          ,DTA_ALTERACAO             ' +
        '          ,DTA_SISTEMA               ' +
        '          ,IND_INATIVO               ' +
        '          ,DES_TELEFONE              ' +
        '          ,DES_FONE_CELULAR          ' +
        '          ,DES_RUA_END               ' +
        '          ,DES_NUM_END               ' +
        '          ,DES_COMPL_END             ' +
        '          ,IND_SIT_CADASTRO          ' +
        '          ,COD_SITUACAO              ' +
        '          ,DTA_ATUALIZACAO_CAD       ' +
        '          ,IND_NOVO_LAYOUT           ' +
        '          ,NUM_EQUIPAMENTO           ' +
        '          ,HORA_CADASTRO             ' +
        '          ,NUM_USUARIO               ' +
        '          ,HORA_ALTERACAO            ' +
        '          ,COD_AUTORIZANTE_ATRASO    ' +
        '          ,DES_CARTAO                ' +
        '          ,COD_OBS                   ' +
        '          ,FONE_ULT_CONTATADO        ' +
        '          ,MELHOR_HORA_CONTATO       ' +
        '          ,COD_EMP_CADASTRO          ' +
        '          ,COD_CARTAO                ' +
        '          ,IND_CLI_UNIFICADO         ' +
        '          ,IND_ATUALIZA_ADM     ' +
        '    from CRE_CLIENTES                ' + '    Where Cod_Emp = ' +
        IntToStr(FrmPrincipal.iCod_Emp) + '    and  Tip_Cliente in (1,2,3,4)  '
        + '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' +
        '    COD_EMP,                         ' +
        '    COD_CLIENTE                      ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := '    select COD_EMP                   ' +
        '          ,' + IntToStr(FrmPrincipal.iCod_unidade) +
        ' AS  COD_UNIDADE_FB ' + '          ,COD_CLIENTE               ' +
        '          ,DES_CLIENTE               ' +
        '          ,DES_FANTASIA              ' +
        '          ,COD_PESSOA                ' +
        '          ,COD_UNIDADE               ' +
        '          ,DES_ENDERECO              ' +
        '          ,DES_PTO_REFER             ' +
        '          ,DES_BAIRRO                ' +
        '          ,NUM_CEP                   ' +
        '          ,COD_CIDADE                ' +
        '          ,NUM_CAIXA_POSTAL          ' +
        '          ,TIP_ENDERECO              ' +
        '          ,IND_MALA_DIRETA           ' +
        '          ,IND_CORREIO               ' +
        '          ,DES_EMAIL                 ' +
        '          ,TIP_PESSOA                ' +
        '          ,DTA_NASCTO                ' +
        '          ,COD_PROFISSAO             ' +
        '          ,TIP_SEXO                  ' +
        '          ,NUM_CPF_CNPJ              ' +
        '          ,NUM_INSCRICAO             ' +
        '          ,TIP_CLIENTE               ' +
        '          ,DTA_CADASTRO              ' +
        '          ,DTA_ALTERACAO             ' +
        '          ,DTA_SISTEMA               ' +
        '          ,IND_INATIVO               ' +
        '          ,DES_TELEFONE              ' +
        '          ,DES_FONE_CELULAR          ' +
        '          ,DES_RUA_END               ' +
        '          ,DES_NUM_END               ' +
        '          ,DES_COMPL_END             ' +
        '          ,IND_SIT_CADASTRO          ' +
        '          ,COD_SITUACAO              ' +
        '          ,DTA_ATUALIZACAO_CAD       ' +
        '          ,IND_NOVO_LAYOUT           ' +
        '          ,NUM_EQUIPAMENTO           ' +
        '          ,HORA_CADASTRO             ' +
        '          ,NUM_USUARIO               ' +
        '          ,HORA_ALTERACAO            ' +
        '          ,COD_AUTORIZANTE_ATRASO    ' +
        '          ,DES_CARTAO                ' +
        '          ,COD_OBS                   ' +
        '          ,FONE_ULT_CONTATADO        ' +
        '          ,MELHOR_HORA_CONTATO       ' +
        '          ,COD_EMP_CADASTRO          ' +
        '          ,COD_CARTAO                ' +
        '          ,IND_CLI_UNIFICADO         ' +
        '          ,IND_ATUALIZA_ADM     ' +
        '    from CRE_CLIENTES                ' + '    Where Cod_Emp = ' +
        IntToStr(FrmPrincipal.iCod_Emp) + ' and  Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' +
        '    COD_EMP,                         ' +
        '    COD_CLIENTE                      ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_CLIENTES ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select COD_EMP  ' + '     ,' +
        IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '     ,CPF_CNPJ     ' + '     ,COD_CARTAO    ' + '     ,DATA_VALIDADE '
        + '     ,IND_BLOQUEIO  ' + '     ,COD_EMP_CARTAO ' +
        '     ,DTA_SISTEMA   ' + '  from CRE_CARTOES   ' +
        ' where  cpf_cnpj in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        ' and Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '                            '
        + '    COD_CLIENTE                     )  ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_CARTOES.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CARTOES';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {
      DtmPGConnection.FDConnection1.Connected := True;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP  ' + '     ,' +
        IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '     ,CPF_CNPJ     ' + '     ,COD_CARTAO    ' + '     ,DATA_VALIDADE '
        + '     ,IND_BLOQUEIO  ' + '     ,COD_EMP_CARTAO ' +
        '     ,DTA_SISTEMA   ' + '  from CRE_CARTOES   ' +
        ' where  cpf_cnpj in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        ' and Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '                            '
        + '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CARTOES';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP  ' + '     ,' +
        IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '     ,CPF_CNPJ     ' + '     ,COD_CARTAO    ' + '     ,DATA_VALIDADE '
        + '     ,IND_BLOQUEIO  ' + '     ,COD_EMP_CARTAO ' +
        '     ,DTA_SISTEMA   ' + '  from CRE_CARTOES   ' +
        ' where  cpf_cnpj in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and  Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '                            '
        + '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CARTOES';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_CARTOES ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    DtmFBConnection.FDConnection1.Offline;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
if (FrmPrincipal.iCod_unidade = 499) or (FrmPrincipal.iCod_unidade = 498) or (FrmPrincipal.iCod_unidade = 53) then
begin   ///////////////////////////////////////////FAZ SOMENTE NAS LOJAS TESTES INICIALMENTE////////////////////////////////////////////
    try
       DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '   select COD_EMP                           ' + '     ,' +
        IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '      ,COD_CLIENTE                       ' +
        '      ,NUM_RG                            ' +
        '      ,DTA_EXP_RG                        ' +
        '      ,DES_ORG_EXP_RG                    ' +
        '      ,COD_CIDADE_NASC                   ' +
        '      ,DES_MAE                           ' +
        '      ,DES_PAI                           ' +
        '      ,TIP_EST_CIVIL                     ' +
        '      ,DTA_CASAMENTO                     ' +
        '      ,QTD_DEPENDENTES                   ' +
        '      ,TIP_RESIDENCIA                    ' +
        '      ,DTA_RESID_ATUAL                   ' +
        '      ,VLR_ALUGUEL                       ' +
        '      ,IND_CONCEITO_ENDER                ' +
        '      ,COD_COMPROV_ENDER                 ' +
        '      ,DES_FONE_RESID                    ' +
        '      ,DES_FONE_CELULAR                  ' +
        '      ,DES_FONE_COMERC                   ' +
        '      ,DES_RAMAL_COMERC                  ' +
        '      ,DES_EMPR_TRAB                     ' +
        '      ,DES_SETOR_TRAB                    ' +
        '      ,DTA_ADMISSAO_TRAB                 ' +
        '      ,NUM_CNPJ_EMPR_TRAB                ' +
        '      ,VLR_RENDA                         ' +
        '      ,VLR_OUTRAS_RENDAS                 ' +
        '      ,COD_COMPROV_RENDA                 ' +
        '      ,VLR_LIMITE_ANT                    ' +
        '      ,VLR_LIMITE                        ' +
        '      ,DTA_ALT_LIMITE                    ' +
        '      ,VLR_CREDITSCORING                 ' +
        '      ,QTD_PTOS_CREDITSCORING            ' +
        '      ,DTA_ULT_CREDITSCORING             ' +
        '      ,DTA_VALIDADE                      ' +
        '      ,NUM_DIA_VCTO                      ' +
        '      ,COD_CARTAO                        ' +
        '      ,COD_SIT_CARTAO                    ' +
        '      ,IND_FUNCIONARIO                   ' +
        '      ,IND_TALAO_CHEQUES                 ' +
        '      ,DTA_ABERTURA_CONTA                ' +
        '      ,COD_BANCO                         ' +
        '      ,IND_CARTAO_CRED                   ' +
        '      ,IND_AUTOMOVEL                     ' +
        '      ,DES_PESSOA_REFER                  ' +
        '      ,DES_FONE_PES_REFER                ' +
        '      ,COD_PARENTESCO                    ' +
        '      ,IND_SPC                           ' +
        '      ,COD_PES_APROV_CAD                 ' +
        '      ,COD_PES_DIGIT_CAD                 ' +
        '      ,DTA_SISTEMA                       ' +
        '      ,TIP_TELEFONE                      ' +
        '      ,DES_CID_NASC                      ' +
        '      ,COD_UF_NASC                       ' +
        '      ,IND_PROFISSAO                     ' +
        '      ,IND_AUTOMOVEL_FINAN               ' +
        '      ,DES_RECADO                        ' +
        '      ,DES_FONE_CELULAR2                 ' +
        '      ,DES_NOME_CARTAO                   ' +
        '      ,DES_OUTRAS_RENDAS                 ' +
        '      ,IND_REEMISSAO_CARTAO              ' +
        '      ,DTA_SOLIC_REEMISAO                ' +
        '      ,DES_PESSOA_REFER2                 ' +
        '      ,DES_FONE_PES_REFER2               ' +
        '      ,COD_PARENTESCO_REF2               ' +
        '      ,COD_MOTIVO_REEMISSAO              ' +
        '      ,IND_PROFISSAO_ADIC                ' +
        '      ,DES_EMPR_TRAB_ADIC                ' +
        '      ,DES_FONE_COMERC_ADIC              ' +
        '      ,DES_RAMAL_COMERC_ADIC             ' +
        '      ,DTA_ADMISSAO_TRAB_ADIC            ' +
        '      ,NUM_CNPJ_EMPR_TRAB_ADIC           ' +
        '      ,DES_SETOR_TRAB_ADIC               ' +
        '      ,IND_MOT_RENDA                     ' +
        '      ,COD_PROFISSAO_ADIC                ' +
        '      ,DES_CELULAR1_ANT                  ' +
        '      ,DES_CELULAR2_ANT                  ' +
        '      ,DES_RESIDENCIAL_ANT               ' +
        '      ,DTA_ALT_FONE                      ' +
        '      ,DIA_RECEBIMENTO                   ' +
        '      ,IND_DELETAR_ADIC                  ' +
        '      ,DTA_CALCULO_CS                    ' +
        '      ,DTA_ALT_RENDA                     ' +
        '      ,IND_POLITICAMENTE_EXPOSTA         ' +
        '      ,DES_PAIS_NACIONALIDADE            ' +
        '      ,IND_ACEITOU_CONTRATO              ' +
        '      ,IND_ACEITOU_NOVIDADES             ' +
        '      ,VERSAO_TERMO_CONTRATO             ' +
        '      ,VERSAO_TERMO_POLITICA             ' +
        '      ,COD_CIDADE_END_EMPRESA            ' +
        '      ,NUM_CEP_END_EMPRESA               ' +
        '      ,DES_RUA_END_EMPRESA               ' +
        '      ,DES_NUM_END_EMPRESA               ' +
        '      ,DES_COMPL_END_EMPRESA             ' +
        '      ,DES_BAIRRO_END_EMPRESA            ' +
        '      ,VLR_LIQ_PATRIMONIO                ' +
        '      ,IND_CONCEITO_GERENTE              ' +
        '      ,COD_VALIDACAO_WPP                 ' +
        '      ,TIP_MENSAGEM                      ' +
        '      ,SCORE_SPC                         ' +
        '  from CRE_CLIENTES_CR                   ' +
        ' where  Cod_Cliente  in ( Select cod_cliente  from CRE_CLIENTES  ' +
                                '  Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
                                '  and  Tip_Cliente in (1,2,3,4) ' +
                                '  and ((Dta_Cadastro >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text) +
                                '  and Dta_Cadastro <= current_date   ' +
                                '  or (Dta_Alteracao >= current_date  - ' + (FrmPrincipal.edtDiasReplicacao.text) +
                                '  and Dta_Alteracao <= current_date))) ' +
        '   order by  COD_CLIENTE ) ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_CLIENTES_CR.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      DtmPGConnection.FDQuery1.RecordCount;
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES_CR';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end; }
       {
      DtmPGConnection.FDConnection1.Connected := True;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '   select COD_EMP                           ' + '     ,' +
        IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '      ,COD_CLIENTE                       ' +
        '      ,NUM_RG                            ' +
        '      ,DTA_EXP_RG                        ' +
        '      ,DES_ORG_EXP_RG                    ' +
        '      ,COD_CIDADE_NASC                   ' +
        '      ,DES_MAE                           ' +
        '      ,DES_PAI                           ' +
        '      ,TIP_EST_CIVIL                     ' +
        '      ,DTA_CASAMENTO                     ' +
        '      ,QTD_DEPENDENTES                   ' +
        '      ,TIP_RESIDENCIA                    ' +
        '      ,DTA_RESID_ATUAL                   ' +
        '      ,VLR_ALUGUEL                       ' +
        '      ,IND_CONCEITO_ENDER                ' +
        '      ,COD_COMPROV_ENDER                 ' +
        '      ,DES_FONE_RESID                    ' +
        '      ,DES_FONE_CELULAR                  ' +
        '      ,DES_FONE_COMERC                   ' +
        '      ,DES_RAMAL_COMERC                  ' +
        '      ,DES_EMPR_TRAB                     ' +
        '      ,DES_SETOR_TRAB                    ' +
        '      ,DTA_ADMISSAO_TRAB                 ' +
        '      ,NUM_CNPJ_EMPR_TRAB                ' +
        '      ,VLR_RENDA                         ' +
        '      ,VLR_OUTRAS_RENDAS                 ' +
        '      ,COD_COMPROV_RENDA                 ' +
        '      ,VLR_LIMITE_ANT                    ' +
        '      ,VLR_LIMITE                        ' +
        '      ,DTA_ALT_LIMITE                    ' +
        '      ,VLR_CREDITSCORING                 ' +
        '      ,QTD_PTOS_CREDITSCORING            ' +
        '      ,DTA_ULT_CREDITSCORING             ' +
        '      ,DTA_VALIDADE                      ' +
        '      ,NUM_DIA_VCTO                      ' +
        '      ,COD_CARTAO                        ' +
        '      ,COD_SIT_CARTAO                    ' +
        '      ,IND_FUNCIONARIO                   ' +
        '      ,IND_TALAO_CHEQUES                 ' +
        '      ,DTA_ABERTURA_CONTA                ' +
        '      ,COD_BANCO                         ' +
        '      ,IND_CARTAO_CRED                   ' +
        '      ,IND_AUTOMOVEL                     ' +
        '      ,DES_PESSOA_REFER                  ' +
        '      ,DES_FONE_PES_REFER                ' +
        '      ,COD_PARENTESCO                    ' +
        '      ,IND_SPC                           ' +
        '      ,COD_PES_APROV_CAD                 ' +
        '      ,COD_PES_DIGIT_CAD                 ' +
        '      ,DTA_SISTEMA                       ' +
        '      ,TIP_TELEFONE                      ' +
        '      ,DES_CID_NASC                      ' +
        '      ,COD_UF_NASC                       ' +
        '      ,IND_PROFISSAO                     ' +
        '      ,IND_AUTOMOVEL_FINAN               ' +
        '      ,DES_RECADO                        ' +
        '      ,DES_FONE_CELULAR2                 ' +
        '      ,DES_NOME_CARTAO                   ' +
        '      ,DES_OUTRAS_RENDAS                 ' +
        '      ,IND_REEMISSAO_CARTAO              ' +
        '      ,DTA_SOLIC_REEMISAO                ' +
        '      ,DES_PESSOA_REFER2                 ' +
        '      ,DES_FONE_PES_REFER2               ' +
        '      ,COD_PARENTESCO_REF2               ' +
        '      ,COD_MOTIVO_REEMISSAO              ' +
        '      ,IND_PROFISSAO_ADIC                ' +
        '      ,DES_EMPR_TRAB_ADIC                ' +
        '      ,DES_FONE_COMERC_ADIC              ' +
        '      ,DES_RAMAL_COMERC_ADIC             ' +
        '      ,DTA_ADMISSAO_TRAB_ADIC            ' +
        '      ,NUM_CNPJ_EMPR_TRAB_ADIC           ' +
        '      ,DES_SETOR_TRAB_ADIC               ' +
        '      ,IND_MOT_RENDA                     ' +
        '      ,COD_PROFISSAO_ADIC                ' +
        '      ,DES_CELULAR1_ANT                  ' +
        '      ,DES_CELULAR2_ANT                  ' +
        '      ,DES_RESIDENCIAL_ANT               ' +
        '      ,DTA_ALT_FONE                      ' +
        '      ,DIA_RECEBIMENTO                   ' +
        '      ,IND_DELETAR_ADIC                  ' +
        '      ,DTA_CALCULO_CS                    ' +
        '      ,DTA_ALT_RENDA                     ' +
        '      ,IND_POLITICAMENTE_EXPOSTA         ' +
        '      ,DES_PAIS_NACIONALIDADE            ' +
        '      ,IND_ACEITOU_CONTRATO              ' +
        '      ,IND_ACEITOU_NOVIDADES             ' +
        '      ,VERSAO_TERMO_CONTRATO             ' +
        '      ,VERSAO_TERMO_POLITICA             ' +
        '      ,COD_CIDADE_END_EMPRESA            ' +
        '      ,NUM_CEP_END_EMPRESA               ' +
        '      ,DES_RUA_END_EMPRESA               ' +
        '      ,DES_NUM_END_EMPRESA               ' +
        '      ,DES_COMPL_END_EMPRESA             ' +
        '      ,DES_BAIRRO_END_EMPRESA            ' +
        '      ,VLR_LIQ_PATRIMONIO                ' +
        '      ,IND_CONCEITO_GERENTE              ' +
        '  from CRE_CLIENTES_CR                      ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and  Tip_Cliente in (1,2,3,4) ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES_CR';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '   select COD_EMP                           ' + '     ,' +
        IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '      ,COD_CLIENTE                       ' +
        '      ,NUM_RG                            ' +
        '      ,DTA_EXP_RG                        ' +
        '      ,DES_ORG_EXP_RG                    ' +
        '      ,COD_CIDADE_NASC                   ' +
        '      ,DES_MAE                           ' +
        '      ,DES_PAI                           ' +
        '      ,TIP_EST_CIVIL                     ' +
        '      ,DTA_CASAMENTO                     ' +
        '      ,QTD_DEPENDENTES                   ' +
        '      ,TIP_RESIDENCIA                    ' +
        '      ,DTA_RESID_ATUAL                   ' +
        '      ,VLR_ALUGUEL                       ' +
        '      ,IND_CONCEITO_ENDER                ' +
        '      ,COD_COMPROV_ENDER                 ' +
        '      ,DES_FONE_RESID                    ' +
        '      ,DES_FONE_CELULAR                  ' +
        '      ,DES_FONE_COMERC                   ' +
        '      ,DES_RAMAL_COMERC                  ' +
        '      ,DES_EMPR_TRAB                     ' +
        '      ,DES_SETOR_TRAB                    ' +
        '      ,DTA_ADMISSAO_TRAB                 ' +
        '      ,NUM_CNPJ_EMPR_TRAB                ' +
        '      ,VLR_RENDA                         ' +
        '      ,VLR_OUTRAS_RENDAS                 ' +
        '      ,COD_COMPROV_RENDA                 ' +
        '      ,VLR_LIMITE_ANT                    ' +
        '      ,VLR_LIMITE                        ' +
        '      ,DTA_ALT_LIMITE                    ' +
        '      ,VLR_CREDITSCORING                 ' +
        '      ,QTD_PTOS_CREDITSCORING            ' +
        '      ,DTA_ULT_CREDITSCORING             ' +
        '      ,DTA_VALIDADE                      ' +
        '      ,NUM_DIA_VCTO                      ' +
        '      ,COD_CARTAO                        ' +
        '      ,COD_SIT_CARTAO                    ' +
        '      ,IND_FUNCIONARIO                   ' +
        '      ,IND_TALAO_CHEQUES                 ' +
        '      ,DTA_ABERTURA_CONTA                ' +
        '      ,COD_BANCO                         ' +
        '      ,IND_CARTAO_CRED                   ' +
        '      ,IND_AUTOMOVEL                     ' +
        '      ,DES_PESSOA_REFER                  ' +
        '      ,DES_FONE_PES_REFER                ' +
        '      ,COD_PARENTESCO                    ' +
        '      ,IND_SPC                           ' +
        '      ,COD_PES_APROV_CAD                 ' +
        '      ,COD_PES_DIGIT_CAD                 ' +
        '      ,DTA_SISTEMA                       ' +
        '      ,TIP_TELEFONE                      ' +
        '      ,DES_CID_NASC                      ' +
        '      ,COD_UF_NASC                       ' +
        '      ,IND_PROFISSAO                     ' +
        '      ,IND_AUTOMOVEL_FINAN               ' +
        '      ,DES_RECADO                        ' +
        '      ,DES_FONE_CELULAR2                 ' +
        '      ,DES_NOME_CARTAO                   ' +
        '      ,DES_OUTRAS_RENDAS                 ' +
        '      ,IND_REEMISSAO_CARTAO              ' +
        '      ,DTA_SOLIC_REEMISAO                ' +
        '      ,DES_PESSOA_REFER2                 ' +
        '      ,DES_FONE_PES_REFER2               ' +
        '      ,COD_PARENTESCO_REF2               ' +
        '      ,COD_MOTIVO_REEMISSAO              ' +
        '      ,IND_PROFISSAO_ADIC                ' +
        '      ,DES_EMPR_TRAB_ADIC                ' +
        '      ,DES_FONE_COMERC_ADIC              ' +
        '      ,DES_RAMAL_COMERC_ADIC             ' +
        '      ,DTA_ADMISSAO_TRAB_ADIC            ' +
        '      ,NUM_CNPJ_EMPR_TRAB_ADIC           ' +
        '      ,DES_SETOR_TRAB_ADIC               ' +
        '      ,IND_MOT_RENDA                     ' +
        '      ,COD_PROFISSAO_ADIC                ' +
        '      ,DES_CELULAR1_ANT                  ' +
        '      ,DES_CELULAR2_ANT                  ' +
        '      ,DES_RESIDENCIAL_ANT               ' +
        '      ,DTA_ALT_FONE                      ' +
        '      ,DIA_RECEBIMENTO                   ' +
        '      ,IND_DELETAR_ADIC                  ' +
        '      ,DTA_CALCULO_CS                    ' +
        '      ,DTA_ALT_RENDA                     ' +
        '      ,IND_POLITICAMENTE_EXPOSTA         ' +
        '      ,DES_PAIS_NACIONALIDADE            ' +
        '      ,IND_ACEITOU_CONTRATO              ' +
        '      ,IND_ACEITOU_NOVIDADES             ' +
        '      ,VERSAO_TERMO_CONTRATO             ' +
        '      ,VERSAO_TERMO_POLITICA             ' +
        '      ,COD_CIDADE_END_EMPRESA            ' +
        '      ,NUM_CEP_END_EMPRESA               ' +
        '      ,DES_RUA_END_EMPRESA               ' +
        '      ,DES_NUM_END_EMPRESA               ' +
        '      ,DES_COMPL_END_EMPRESA             ' +
        '      ,DES_BAIRRO_END_EMPRESA            ' +
        '      ,VLR_LIQ_PATRIMONIO                ' +
        '      ,IND_CONCEITO_GERENTE              ' +
        '  from CRE_CLIENTES_CR                      ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES_CR';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_CLIENTES_CR ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
end
else
begin
     try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '   select COD_EMP                           ' + '     ,' +
      IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '      ,COD_CLIENTE                       ' +
        '      ,NUM_RG                            ' +
        '      ,DTA_EXP_RG                        ' +
        '      ,DES_ORG_EXP_RG                    ' +
        '      ,COD_CIDADE_NASC                   ' +
        '      ,DES_MAE                           ' +
        '      ,DES_PAI                           ' +
        '      ,TIP_EST_CIVIL                     ' +
        '      ,DTA_CASAMENTO                     ' +
        '      ,QTD_DEPENDENTES                   ' +
        '      ,TIP_RESIDENCIA                    ' +
        '      ,DTA_RESID_ATUAL                   ' +
        '      ,VLR_ALUGUEL                       ' +
        '      ,IND_CONCEITO_ENDER                ' +
        '      ,COD_COMPROV_ENDER                 ' +
        '      ,DES_FONE_RESID                    ' +
        '      ,DES_FONE_CELULAR                  ' +
        '      ,DES_FONE_COMERC                   ' +
        '      ,DES_RAMAL_COMERC                  ' +
        '      ,DES_EMPR_TRAB                     ' +
        '      ,DES_SETOR_TRAB                    ' +
        '      ,DTA_ADMISSAO_TRAB                 ' +
        '      ,NUM_CNPJ_EMPR_TRAB                ' +
        '      ,VLR_RENDA                         ' +
        '      ,VLR_OUTRAS_RENDAS                 ' +
        '      ,COD_COMPROV_RENDA                 ' +
        '      ,VLR_LIMITE_ANT                    ' +
        '      ,VLR_LIMITE                        ' +
        '      ,DTA_ALT_LIMITE                    ' +
        '      ,VLR_CREDITSCORING                 ' +
        '      ,QTD_PTOS_CREDITSCORING            ' +
        '      ,DTA_ULT_CREDITSCORING             ' +
        '      ,DTA_VALIDADE                      ' +
        '      ,NUM_DIA_VCTO                      ' +
        '      ,COD_CARTAO                        ' +
        '      ,COD_SIT_CARTAO                    ' +
        '      ,IND_FUNCIONARIO                   ' +
        '      ,IND_TALAO_CHEQUES                 ' +
        '      ,DTA_ABERTURA_CONTA                ' +
        '      ,COD_BANCO                         ' +
        '      ,IND_CARTAO_CRED                   ' +
        '      ,IND_AUTOMOVEL                     ' +
        '      ,DES_PESSOA_REFER                  ' +
        '      ,DES_FONE_PES_REFER                ' +
        '      ,COD_PARENTESCO                    ' +
        '      ,IND_SPC                           ' +
        '      ,COD_PES_APROV_CAD                 ' +
        '      ,COD_PES_DIGIT_CAD                 ' +
        '      ,DTA_SISTEMA                       ' +
        '      ,TIP_TELEFONE                      ' +
        '      ,DES_CID_NASC                      ' +
        '      ,COD_UF_NASC                       ' +
        '      ,IND_PROFISSAO                     ' +
        '      ,IND_AUTOMOVEL_FINAN               ' +
        '      ,DES_RECADO                        ' +
        '      ,DES_FONE_CELULAR2                 ' +
        '      ,DES_NOME_CARTAO                   ' +
        '      ,DES_OUTRAS_RENDAS                 ' +
        '      ,IND_REEMISSAO_CARTAO              ' +
        '      ,DTA_SOLIC_REEMISAO                ' +
        '      ,DES_PESSOA_REFER2                 ' +
        '      ,DES_FONE_PES_REFER2               ' +
        '      ,COD_PARENTESCO_REF2               ' +
        '      ,COD_MOTIVO_REEMISSAO              ' +
        '      ,IND_PROFISSAO_ADIC                ' +
        '      ,DES_EMPR_TRAB_ADIC                ' +
        '      ,DES_FONE_COMERC_ADIC              ' +
        '      ,DES_RAMAL_COMERC_ADIC             ' +
        '      ,DTA_ADMISSAO_TRAB_ADIC            ' +
        '      ,NUM_CNPJ_EMPR_TRAB_ADIC           ' +
        '      ,DES_SETOR_TRAB_ADIC               ' +
        '      ,IND_MOT_RENDA                     ' +
        '      ,COD_PROFISSAO_ADIC                ' +
        '      ,DES_CELULAR1_ANT                  ' +
        '      ,DES_CELULAR2_ANT                  ' +
        '      ,DES_RESIDENCIAL_ANT               ' +
        '      ,DTA_ALT_FONE                      ' +
        '      ,DIA_RECEBIMENTO                   ' +
        '      ,IND_DELETAR_ADIC                  ' +
        '      ,DTA_CALCULO_CS                    ' +
        '      ,DTA_ALT_RENDA                     ' +
        '      ,IND_POLITICAMENTE_EXPOSTA         ' +
        '      ,DES_PAIS_NACIONALIDADE            ' +
        '      ,IND_ACEITOU_CONTRATO              ' +
        '      ,IND_ACEITOU_NOVIDADES             ' +
        '      ,VERSAO_TERMO_CONTRATO             ' +
        '      ,VERSAO_TERMO_POLITICA             ' +
        '      ,COD_CIDADE_END_EMPRESA            ' +
        '      ,NUM_CEP_END_EMPRESA               ' +
        '      ,DES_RUA_END_EMPRESA               ' +
        '      ,DES_NUM_END_EMPRESA               ' +
        '      ,DES_COMPL_END_EMPRESA             ' +
        '      ,DES_BAIRRO_END_EMPRESA            ' +
        '      ,VLR_LIQ_PATRIMONIO                ' +
        '      ,IND_CONCEITO_GERENTE              ' +
        '  from CRE_CLIENTES_CR                   ' +
        ' where  Cod_Cliente  in ( Select cod_cliente  from CRE_CLIENTES  ' +
                                '  Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
                                '  and  Tip_Cliente in (1,2,3,4) ' +
                                '  and ((Dta_Cadastro >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text) +
                                '  and Dta_Cadastro <= current_date   ' +
                                '  or (Dta_Alteracao >= current_date  - ' + (FrmPrincipal.edtDiasReplicacao.text) +
                                '  and Dta_Alteracao <= current_date))) ' +
        '   order by  COD_CLIENTE ) ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_CLIENTES_CR.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      DtmPGConnection.FDQuery1.RecordCount;
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES_CR';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;  }
       {
      DtmPGConnection.FDConnection1.Connected := True;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '   select COD_EMP                           ' + '     ,' +
        IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '      ,COD_CLIENTE                       ' +
        '      ,NUM_RG                            ' +
        '      ,DTA_EXP_RG                        ' +
        '      ,DES_ORG_EXP_RG                    ' +
        '      ,COD_CIDADE_NASC                   ' +
        '      ,DES_MAE                           ' +
        '      ,DES_PAI                           ' +
        '      ,TIP_EST_CIVIL                     ' +
        '      ,DTA_CASAMENTO                     ' +
        '      ,QTD_DEPENDENTES                   ' +
        '      ,TIP_RESIDENCIA                    ' +
        '      ,DTA_RESID_ATUAL                   ' +
        '      ,VLR_ALUGUEL                       ' +
        '      ,IND_CONCEITO_ENDER                ' +
        '      ,COD_COMPROV_ENDER                 ' +
        '      ,DES_FONE_RESID                    ' +
        '      ,DES_FONE_CELULAR                  ' +
        '      ,DES_FONE_COMERC                   ' +
        '      ,DES_RAMAL_COMERC                  ' +
        '      ,DES_EMPR_TRAB                     ' +
        '      ,DES_SETOR_TRAB                    ' +
        '      ,DTA_ADMISSAO_TRAB                 ' +
        '      ,NUM_CNPJ_EMPR_TRAB                ' +
        '      ,VLR_RENDA                         ' +
        '      ,VLR_OUTRAS_RENDAS                 ' +
        '      ,COD_COMPROV_RENDA                 ' +
        '      ,VLR_LIMITE_ANT                    ' +
        '      ,VLR_LIMITE                        ' +
        '      ,DTA_ALT_LIMITE                    ' +
        '      ,VLR_CREDITSCORING                 ' +
        '      ,QTD_PTOS_CREDITSCORING            ' +
        '      ,DTA_ULT_CREDITSCORING             ' +
        '      ,DTA_VALIDADE                      ' +
        '      ,NUM_DIA_VCTO                      ' +
        '      ,COD_CARTAO                        ' +
        '      ,COD_SIT_CARTAO                    ' +
        '      ,IND_FUNCIONARIO                   ' +
        '      ,IND_TALAO_CHEQUES                 ' +
        '      ,DTA_ABERTURA_CONTA                ' +
        '      ,COD_BANCO                         ' +
        '      ,IND_CARTAO_CRED                   ' +
        '      ,IND_AUTOMOVEL                     ' +
        '      ,DES_PESSOA_REFER                  ' +
        '      ,DES_FONE_PES_REFER                ' +
        '      ,COD_PARENTESCO                    ' +
        '      ,IND_SPC                           ' +
        '      ,COD_PES_APROV_CAD                 ' +
        '      ,COD_PES_DIGIT_CAD                 ' +
        '      ,DTA_SISTEMA                       ' +
        '      ,TIP_TELEFONE                      ' +
        '      ,DES_CID_NASC                      ' +
        '      ,COD_UF_NASC                       ' +
        '      ,IND_PROFISSAO                     ' +
        '      ,IND_AUTOMOVEL_FINAN               ' +
        '      ,DES_RECADO                        ' +
        '      ,DES_FONE_CELULAR2                 ' +
        '      ,DES_NOME_CARTAO                   ' +
        '      ,DES_OUTRAS_RENDAS                 ' +
        '      ,IND_REEMISSAO_CARTAO              ' +
        '      ,DTA_SOLIC_REEMISAO                ' +
        '      ,DES_PESSOA_REFER2                 ' +
        '      ,DES_FONE_PES_REFER2               ' +
        '      ,COD_PARENTESCO_REF2               ' +
        '      ,COD_MOTIVO_REEMISSAO              ' +
        '      ,IND_PROFISSAO_ADIC                ' +
        '      ,DES_EMPR_TRAB_ADIC                ' +
        '      ,DES_FONE_COMERC_ADIC              ' +
        '      ,DES_RAMAL_COMERC_ADIC             ' +
        '      ,DTA_ADMISSAO_TRAB_ADIC            ' +
        '      ,NUM_CNPJ_EMPR_TRAB_ADIC           ' +
        '      ,DES_SETOR_TRAB_ADIC               ' +
        '      ,IND_MOT_RENDA                     ' +
        '      ,COD_PROFISSAO_ADIC                ' +
        '      ,DES_CELULAR1_ANT                  ' +
        '      ,DES_CELULAR2_ANT                  ' +
        '      ,DES_RESIDENCIAL_ANT               ' +
        '      ,DTA_ALT_FONE                      ' +
        '      ,DIA_RECEBIMENTO                   ' +
        '      ,IND_DELETAR_ADIC                  ' +
        '      ,DTA_CALCULO_CS                    ' +
        '      ,DTA_ALT_RENDA                     ' +
        '      ,IND_POLITICAMENTE_EXPOSTA         ' +
        '      ,DES_PAIS_NACIONALIDADE            ' +
        '      ,IND_ACEITOU_CONTRATO              ' +
        '      ,IND_ACEITOU_NOVIDADES             ' +
        '      ,VERSAO_TERMO_CONTRATO             ' +
        '      ,VERSAO_TERMO_POLITICA             ' +
        '      ,COD_CIDADE_END_EMPRESA            ' +
        '      ,NUM_CEP_END_EMPRESA               ' +
        '      ,DES_RUA_END_EMPRESA               ' +
        '      ,DES_NUM_END_EMPRESA               ' +
        '      ,DES_COMPL_END_EMPRESA             ' +
        '      ,DES_BAIRRO_END_EMPRESA            ' +
        '      ,VLR_LIQ_PATRIMONIO                ' +
        '      ,IND_CONCEITO_GERENTE              ' +
        '  from CRE_CLIENTES_CR                      ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and  Tip_Cliente in (1,2,3,4) ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES_CR';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '   select COD_EMP                           ' + '     ,' +
        IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '      ,COD_CLIENTE                       ' +
        '      ,NUM_RG                            ' +
        '      ,DTA_EXP_RG                        ' +
        '      ,DES_ORG_EXP_RG                    ' +
        '      ,COD_CIDADE_NASC                   ' +
        '      ,DES_MAE                           ' +
        '      ,DES_PAI                           ' +
        '      ,TIP_EST_CIVIL                     ' +
        '      ,DTA_CASAMENTO                     ' +
        '      ,QTD_DEPENDENTES                   ' +
        '      ,TIP_RESIDENCIA                    ' +
        '      ,DTA_RESID_ATUAL                   ' +
        '      ,VLR_ALUGUEL                       ' +
        '      ,IND_CONCEITO_ENDER                ' +
        '      ,COD_COMPROV_ENDER                 ' +
        '      ,DES_FONE_RESID                    ' +
        '      ,DES_FONE_CELULAR                  ' +
        '      ,DES_FONE_COMERC                   ' +
        '      ,DES_RAMAL_COMERC                  ' +
        '      ,DES_EMPR_TRAB                     ' +
        '      ,DES_SETOR_TRAB                    ' +
        '      ,DTA_ADMISSAO_TRAB                 ' +
        '      ,NUM_CNPJ_EMPR_TRAB                ' +
        '      ,VLR_RENDA                         ' +
        '      ,VLR_OUTRAS_RENDAS                 ' +
        '      ,COD_COMPROV_RENDA                 ' +
        '      ,VLR_LIMITE_ANT                    ' +
        '      ,VLR_LIMITE                        ' +
        '      ,DTA_ALT_LIMITE                    ' +
        '      ,VLR_CREDITSCORING                 ' +
        '      ,QTD_PTOS_CREDITSCORING            ' +
        '      ,DTA_ULT_CREDITSCORING             ' +
        '      ,DTA_VALIDADE                      ' +
        '      ,NUM_DIA_VCTO                      ' +
        '      ,COD_CARTAO                        ' +
        '      ,COD_SIT_CARTAO                    ' +
        '      ,IND_FUNCIONARIO                   ' +
        '      ,IND_TALAO_CHEQUES                 ' +
        '      ,DTA_ABERTURA_CONTA                ' +
        '      ,COD_BANCO                         ' +
        '      ,IND_CARTAO_CRED                   ' +
        '      ,IND_AUTOMOVEL                     ' +
        '      ,DES_PESSOA_REFER                  ' +
        '      ,DES_FONE_PES_REFER                ' +
        '      ,COD_PARENTESCO                    ' +
        '      ,IND_SPC                           ' +
        '      ,COD_PES_APROV_CAD                 ' +
        '      ,COD_PES_DIGIT_CAD                 ' +
        '      ,DTA_SISTEMA                       ' +
        '      ,TIP_TELEFONE                      ' +
        '      ,DES_CID_NASC                      ' +
        '      ,COD_UF_NASC                       ' +
        '      ,IND_PROFISSAO                     ' +
        '      ,IND_AUTOMOVEL_FINAN               ' +
        '      ,DES_RECADO                        ' +
        '      ,DES_FONE_CELULAR2                 ' +
        '      ,DES_NOME_CARTAO                   ' +
        '      ,DES_OUTRAS_RENDAS                 ' +
        '      ,IND_REEMISSAO_CARTAO              ' +
        '      ,DTA_SOLIC_REEMISAO                ' +
        '      ,DES_PESSOA_REFER2                 ' +
        '      ,DES_FONE_PES_REFER2               ' +
        '      ,COD_PARENTESCO_REF2               ' +
        '      ,COD_MOTIVO_REEMISSAO              ' +
        '      ,IND_PROFISSAO_ADIC                ' +
        '      ,DES_EMPR_TRAB_ADIC                ' +
        '      ,DES_FONE_COMERC_ADIC              ' +
        '      ,DES_RAMAL_COMERC_ADIC             ' +
        '      ,DTA_ADMISSAO_TRAB_ADIC            ' +
        '      ,NUM_CNPJ_EMPR_TRAB_ADIC           ' +
        '      ,DES_SETOR_TRAB_ADIC               ' +
        '      ,IND_MOT_RENDA                     ' +
        '      ,COD_PROFISSAO_ADIC                ' +
        '      ,DES_CELULAR1_ANT                  ' +
        '      ,DES_CELULAR2_ANT                  ' +
        '      ,DES_RESIDENCIAL_ANT               ' +
        '      ,DTA_ALT_FONE                      ' +
        '      ,DIA_RECEBIMENTO                   ' +
        '      ,IND_DELETAR_ADIC                  ' +
        '      ,DTA_CALCULO_CS                    ' +
        '      ,DTA_ALT_RENDA                     ' +
        '      ,IND_POLITICAMENTE_EXPOSTA         ' +
        '      ,DES_PAIS_NACIONALIDADE            ' +
        '      ,IND_ACEITOU_CONTRATO              ' +
        '      ,IND_ACEITOU_NOVIDADES             ' +
        '      ,VERSAO_TERMO_CONTRATO             ' +
        '      ,VERSAO_TERMO_POLITICA             ' +
        '      ,COD_CIDADE_END_EMPRESA            ' +
        '      ,NUM_CEP_END_EMPRESA               ' +
        '      ,DES_RUA_END_EMPRESA               ' +
        '      ,DES_NUM_END_EMPRESA               ' +
        '      ,DES_COMPL_END_EMPRESA             ' +
        '      ,DES_BAIRRO_END_EMPRESA            ' +
        '      ,VLR_LIQ_PATRIMONIO                ' +
        '      ,IND_CONCEITO_GERENTE              ' +
        '  from CRE_CLIENTES_CR                      ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_CLIENTES_CR';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_CLIENTES_CR ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '  select COD_EMP                       '
        + '     ,' + IntToStr(FrmPrincipal.iCod_unidade) +
        ' AS  COD_UNIDADE_FB ' + '        ,COD_CLIENTE                   ' +
        '        ,NUM_ADICIONAL                 ' +
        '        ,COD_CARTAO_ADIC               ' +
        '        ,IND_EMITE_CARTAO              ' +
        '        ,DTA_NASCTO                    ' +
        '        ,TIP_SEXO                      ' +
        '        ,NUM_CPF_CNPJ                  ' +
        '        ,NUM_RG                        ' +
        '        ,DTA_EXP_RG                    ' +
        '        ,DES_ORG_EXP_RG                ' +
        '        ,COD_PROFISSAO                 ' +
        '        ,VLR_RENDA                     ' +
        '        ,COD_PARENTESCO                ' +
        '        ,DES_MAE                       ' +
        '        ,IND_SPC                       ' +
        '        ,DES_EMPR_TRAB                 ' +
        '        ,DES_TELEFONE                  ' +
        '        ,DES_RAMAL_FONE                ' +
        '        ,DTA_SISTEMA                   ' +
        '        ,DES_PESSOA                    ' +
        '        ,COD_SIT_CARTAO_ADIC           ' +
        '        ,IND_COMP_RENDA                ' +
        '        ,IND_PROFISSAO                 ' +
        '        ,DTA_ADMISSAO_TRAB             ' +
        '        ,NOME_AUT_CARTAO               ' +
        '        ,IND_REEMISSAO_CARTAO          ' +
        '        ,DTA_SOLIC_REEMISAO            ' +
        '  from CRE_PESSOA_AUTORIZADA           ' + ' where  Cod_Cliente  in ( '
        + '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and Tip_Cliente in (1,2,3,4) ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_PESSOA_AUTORIZADA.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_PESSOA_AUTORIZADA';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;  }
       {
      DtmPGConnection.FDConnection1.Connected := True;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := '  select COD_EMP                       '
        + '     ,' + IntToStr(FrmPrincipal.iCod_unidade) +
        ' AS  COD_UNIDADE_FB ' + '        ,COD_CLIENTE                   ' +
        '        ,NUM_ADICIONAL                 ' +
        '        ,COD_CARTAO_ADIC               ' +
        '        ,IND_EMITE_CARTAO              ' +
        '        ,DTA_NASCTO                    ' +
        '        ,TIP_SEXO                      ' +
        '        ,NUM_CPF_CNPJ                  ' +
        '        ,NUM_RG                        ' +
        '        ,DTA_EXP_RG                    ' +
        '        ,DES_ORG_EXP_RG                ' +
        '        ,COD_PROFISSAO                 ' +
        '        ,VLR_RENDA                     ' +
        '        ,COD_PARENTESCO                ' +
        '        ,DES_MAE                       ' +
        '        ,IND_SPC                       ' +
        '        ,DES_EMPR_TRAB                 ' +
        '        ,DES_TELEFONE                  ' +
        '        ,DES_RAMAL_FONE                ' +
        '        ,DTA_SISTEMA                   ' +
        '        ,DES_PESSOA                    ' +
        '        ,COD_SIT_CARTAO_ADIC           ' +
        '        ,IND_COMP_RENDA                ' +
        '        ,IND_PROFISSAO                 ' +
        '        ,DTA_ADMISSAO_TRAB             ' +
        '        ,NOME_AUT_CARTAO               ' +
        '        ,IND_REEMISSAO_CARTAO          ' +
        '        ,DTA_SOLIC_REEMISAO            ' +
        '  from CRE_PESSOA_AUTORIZADA           ' + ' where  Cod_Cliente  in ( '
        + '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and Tip_Cliente in (1,2,3,4) ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_PESSOA_AUTORIZADA';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := '  select COD_EMP                       '
        + '     ,' + IntToStr(FrmPrincipal.iCod_unidade) +
        ' AS  COD_UNIDADE_FB ' + '        ,COD_CLIENTE                   ' +
        '        ,NUM_ADICIONAL                 ' +
        '        ,COD_CARTAO_ADIC               ' +
        '        ,IND_EMITE_CARTAO              ' +
        '        ,DTA_NASCTO                    ' +
        '        ,TIP_SEXO                      ' +
        '        ,NUM_CPF_CNPJ                  ' +
        '        ,NUM_RG                        ' +
        '        ,DTA_EXP_RG                    ' +
        '        ,DES_ORG_EXP_RG                ' +
        '        ,COD_PROFISSAO                 ' +
        '        ,VLR_RENDA                     ' +
        '        ,COD_PARENTESCO                ' +
        '        ,DES_MAE                       ' +
        '        ,IND_SPC                       ' +
        '        ,DES_EMPR_TRAB                 ' +
        '        ,DES_TELEFONE                  ' +
        '        ,DES_RAMAL_FONE                ' +
        '        ,DTA_SISTEMA                   ' +
        '        ,DES_PESSOA                    ' +
        '        ,COD_SIT_CARTAO_ADIC           ' +
        '        ,IND_COMP_RENDA                ' +
        '        ,IND_PROFISSAO                 ' +
        '        ,DTA_ADMISSAO_TRAB             ' +
        '        ,NOME_AUT_CARTAO               ' +
        '        ,IND_REEMISSAO_CARTAO          ' +
        '        ,DTA_SOLIC_REEMISAO            ' +
        '  from CRE_PESSOA_AUTORIZADA           ' + ' where  Cod_Cliente  in ( '
        + '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and  Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_PESSOA_AUTORIZADA';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_PESSOA_AUTORIZADA ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString :=  'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select COD_EMP               ' +
        '     ,' + IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB '
        + '       ,COD_CLIENTE           ' + '       ,NUM_SEQ               ' +
        '       ,DES_COMENTARIO        ' + '       ,DTA_SISTEMA           ' +
        '       ,COD_NIVEL             ' + ' from CRE_COMENTARIOS_CLI     ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and  Tip_Cliente in (1,2,3,4) ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_COMENTARIOS_CLI.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_COMENTARIOS_CLI';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      }
      {DtmPGConnection.FDConnection1.Connected := True;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP               ' +
        '     ,' + IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB '
        + '       ,COD_CLIENTE           ' + '       ,NUM_SEQ               ' +
        '       ,DES_COMENTARIO        ' + '       ,DTA_SISTEMA           ' +
        '       ,COD_NIVEL             ' + ' from CRE_COMENTARIOS_CLI     ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and  Tip_Cliente in (1,2,3,4) ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_COMENTARIOS_CLI';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP               ' +
        '     ,' + IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB '
        + '       ,COD_CLIENTE           ' + '       ,NUM_SEQ               ' +
        '       ,DES_COMENTARIO        ' + '       ,DTA_SISTEMA           ' +
        '       ,COD_NIVEL             ' + ' from CRE_COMENTARIOS_CLI     ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_COMENTARIOS_CLI';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_COMENTARIOS_CLI ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select COD_EMP            ' + '     ,'
        + IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '       ,COD_CLIENTE        ' + '       ,NUM_SEQ            ' +
        '       ,DES_OBSERVACAO     ' + '       ,DES_RESPONSAVEL    ' +
        '       ,DTA_SISTEMA        ' + ' from CRE_OBSERVACAO_CLI     ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_OBSERVACAO_CLI.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_OBSERVACAO_CLI';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }

      {
      DtmPGConnection.FDConnection1.Connected := True;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP            ' + '     ,'
        + IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '       ,COD_CLIENTE        ' + '       ,NUM_SEQ            ' +
        '       ,DES_OBSERVACAO     ' + '       ,DES_RESPONSAVEL    ' +
        '       ,DTA_SISTEMA        ' + ' from CRE_OBSERVACAO_CLI     ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_OBSERVACAO_CLI';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select COD_EMP            ' + '     ,'
        + IntToStr(FrmPrincipal.iCod_unidade) + ' AS  COD_UNIDADE_FB ' +
        '       ,COD_CLIENTE        ' + '       ,NUM_SEQ            ' +
        '       ,DES_OBSERVACAO     ' + '       ,DES_RESPONSAVEL    ' +
        '       ,DTA_SISTEMA        ' + ' from CRE_OBSERVACAO_CLI     ' +
        ' where  Cod_Cliente  in ( ' +
        '  Select cod_cliente  from CRE_CLIENTES                ' +
        '    Where Cod_Emp = ' + IntToStr(FrmPrincipal.iCod_Emp) +
        '    and Tip_Cliente in (1,2,3,4)  ' +
        '                    and ((Dta_Cadastro >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Cadastro <= current_date   ' +
        '                    or (Dta_Alteracao >= current_date  - ' +
        (FrmPrincipal.edtDiasReplicacao.text) +
        '                    and Dta_Alteracao <= current_date))) ' +
        '    order by                         ' + '     ' +
        '    COD_CLIENTE                     )  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_OBSERVACAO_CLI';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_OBSERVACAO_CLI ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' + ' from CRE_RECEBIMENTOS '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (sDiasRecebimento)+
        ' and  cod_contrato_reneg is null ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_RECEBIMENTOS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_RECEBIMENTO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
       }
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' + ' from CRE_RECEBIMENTOS a '
        + ' where a.DTA_MOVIMENTO >= current_date - ' +
        (sDiasRecebimento)+
        ' and a.cod_contrato_reneg is not null '+
        ' and a.cod_unidade = (select cod_unidade from ger_configuracao) '  +
        ' and exists (    '+
        ' select 1 from cre_contas_receber b  '+
        ' where a.cod_emp = b.cod_emp           '+
        ' and a.cod_unidade = b.cod_unidade       '+
        ' and a.cod_contrato_reneg = b.cod_contrato  '+
        '  )';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_RECEBIMENTOS_RENEGOCIACAO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_RECEBIMENTO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
           }
       {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CRE_RECEBIMENTOS '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_RECEBIMENTO';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from CRE_RECEBIMENTOS '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_RECEBIMENTO';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_RECEBIMENTOS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text :=  ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                               + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat :=  (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := ' Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;



    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat := (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString :=  'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' +
        ' from EST_CARTAO_PRESENTE_RETORNADO ' +
        ' where DTA_RETORNADO >= current_date -  ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CARTAO_PRESENTE_RETORNADO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;


      {
      QueryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CARTAO_PRESENTE_RET';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }

       {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from EST_CARTAO_PRESENTE_RETORNADO ' +
        ' where DTA_RETORNADO >= current_date -  ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CARTAO_PRESENTE_RET';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from EST_CARTAO_PRESENTE_RETORNADO ' +
        ' where DTA_RETORNADO >= current_date -  ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CARTAO_PRESENTE_RET';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CARTAO_PRESENTE_RET ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '  select GER_MVTO_CUPOM_DESCTO.COD_EMP                                                  '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.COD_UNIDADE          '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_EQUIPAMENTO      '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.DTA_MOVIMENTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_CUPOM            '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_CPF_CNPJ         '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.HOR_MOVIMENTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.TIPO_DESCONTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.CUPOM_DESCONTO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.VALOR_DESCONTO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.TIPO_VENDA           '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.IND_ATUALIZADO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.DTA_ATUA_ADM         '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.HORA_ATUA_ADM        '
        + '                                      from GER_MVTO_CUPOM_DESCTO                        '
        + '                                      where DTA_MOVIMENTO >= current_date - '
        + (FrmPrincipal.edtDiasReplicacao.text) +
        '                                      order by                                          '
        + '                                      GER_MVTO_CUPOM_DESCTO.COD_EMP,                    '
        + '                                      GER_MVTO_CUPOM_DESCTO.COD_UNIDADE,                '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_EQUIPAMENTO,            '
        + '                                      GER_MVTO_CUPOM_DESCTO.DTA_MOVIMENTO,              '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_CUPOM,                  '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_CPF_CNPJ,               '
        + '                                      GER_MVTO_CUPOM_DESCTO.HOR_MOVIMENTO               ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_MVTO_CUPOM_DESCTO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_MVTO_CUPOM_DESCTO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }

      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  select GER_MVTO_CUPOM_DESCTO.COD_EMP                                                  '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.COD_UNIDADE          '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_EQUIPAMENTO      '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.DTA_MOVIMENTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_CUPOM            '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_CPF_CNPJ         '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.HOR_MOVIMENTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.TIPO_DESCONTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.CUPOM_DESCONTO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.VALOR_DESCONTO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.TIPO_VENDA           '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.IND_ATUALIZADO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.DTA_ATUA_ADM         '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.HORA_ATUA_ADM        '
        + '                                      from GER_MVTO_CUPOM_DESCTO                        '
        + '                                      where DTA_MOVIMENTO >= current_date - '
        + (FrmPrincipal.edtDiasReplicacao.text) +
        '                                      order by                                          '
        + '                                      GER_MVTO_CUPOM_DESCTO.COD_EMP,                    '
        + '                                      GER_MVTO_CUPOM_DESCTO.COD_UNIDADE,                '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_EQUIPAMENTO,            '
        + '                                      GER_MVTO_CUPOM_DESCTO.DTA_MOVIMENTO,              '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_CUPOM,                  '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_CPF_CNPJ,               '
        + '                                      GER_MVTO_CUPOM_DESCTO.HOR_MOVIMENTO               ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_MVTO_CUPOM_DESCTO';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  select GER_MVTO_CUPOM_DESCTO.COD_EMP                                                  '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.COD_UNIDADE          '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_EQUIPAMENTO      '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.DTA_MOVIMENTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_CUPOM            '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.NUM_CPF_CNPJ         '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.HOR_MOVIMENTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.TIPO_DESCONTO        '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.CUPOM_DESCONTO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.VALOR_DESCONTO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.TIPO_VENDA           '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.IND_ATUALIZADO       '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.DTA_ATUA_ADM         '
        + '                                            ,GER_MVTO_CUPOM_DESCTO.HORA_ATUA_ADM        '
        + '                                      from GER_MVTO_CUPOM_DESCTO                        '
        + '                                      where DTA_MOVIMENTO >= current_date - '
        + (FrmPrincipal.edtDiasReplicacao.text) +
        '                                      order by                                          '
        + '                                      GER_MVTO_CUPOM_DESCTO.COD_EMP,                    '
        + '                                      GER_MVTO_CUPOM_DESCTO.COD_UNIDADE,                '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_EQUIPAMENTO,            '
        + '                                      GER_MVTO_CUPOM_DESCTO.DTA_MOVIMENTO,              '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_CUPOM,                  '
        + '                                      GER_MVTO_CUPOM_DESCTO.NUM_CPF_CNPJ,               '
        + '                                      GER_MVTO_CUPOM_DESCTO.HOR_MOVIMENTO               ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_MVTO_CUPOM_DESCTO';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_MVTO_CUPOM_DESCTO ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '  select NFI_PRE_TRANSFERENCIA.COD_EMP        ' +
        '        ,NFI_PRE_TRANSFERENCIA.COD_UNIDADE    ' +
        '        ,NFI_PRE_TRANSFERENCIA.DTA_MVTO       ' +
        '        ,NFI_PRE_TRANSFERENCIA.COD_ITEM       ' +
        '        ,NFI_PRE_TRANSFERENCIA.QTD_INVENTARIO ' +
        '        ,NFI_PRE_TRANSFERENCIA.QTD_LANCAMENTO ' +
        '        ,NFI_PRE_TRANSFERENCIA.NUM_SEQ_NF     ' +
        '        ,NFI_PRE_TRANSFERENCIA.DTA_SISTEMA    ' +
        '  from NFI_PRE_TRANSFERENCIA                  ' +
        '  WHERE DTA_MVTO >= CURRENT_DATE - 60         ' +
        '  order by                                    ' +
        '  NFI_PRE_TRANSFERENCIA.COD_EMP,              ' +
        '  NFI_PRE_TRANSFERENCIA.COD_UNIDADE,          ' +
        '  NFI_PRE_TRANSFERENCIA.DTA_MVTO,             ' +
        '  NFI_PRE_TRANSFERENCIA.COD_ITEM              ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_PRE_TRANSFERENCIA.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;


      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_PRE_TRANSFERENCIA';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;

      }

       {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  select NFI_PRE_TRANSFERENCIA.COD_EMP        ' +
        '        ,NFI_PRE_TRANSFERENCIA.COD_UNIDADE    ' +
        '        ,NFI_PRE_TRANSFERENCIA.DTA_MVTO       ' +
        '        ,NFI_PRE_TRANSFERENCIA.COD_ITEM       ' +
        '        ,NFI_PRE_TRANSFERENCIA.QTD_INVENTARIO ' +
        '        ,NFI_PRE_TRANSFERENCIA.QTD_LANCAMENTO ' +
        '        ,NFI_PRE_TRANSFERENCIA.NUM_SEQ_NF     ' +
        '        ,NFI_PRE_TRANSFERENCIA.DTA_SISTEMA    ' +
        '  from NFI_PRE_TRANSFERENCIA                  ' +
        '  WHERE DTA_MVTO >= CURRENT_DATE - 60         ' +
        '  order by                                    ' +
        '  NFI_PRE_TRANSFERENCIA.COD_EMP,              ' +
        '  NFI_PRE_TRANSFERENCIA.COD_UNIDADE,          ' +
        '  NFI_PRE_TRANSFERENCIA.DTA_MVTO,             ' +
        '  NFI_PRE_TRANSFERENCIA.COD_ITEM              ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_PRE_TRANSFERENCIA';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=
        '  select NFI_PRE_TRANSFERENCIA.COD_EMP        ' +
        '        ,NFI_PRE_TRANSFERENCIA.COD_UNIDADE    ' +
        '        ,NFI_PRE_TRANSFERENCIA.DTA_MVTO       ' +
        '        ,NFI_PRE_TRANSFERENCIA.COD_ITEM       ' +
        '        ,NFI_PRE_TRANSFERENCIA.QTD_INVENTARIO ' +
        '        ,NFI_PRE_TRANSFERENCIA.QTD_LANCAMENTO ' +
        '        ,NFI_PRE_TRANSFERENCIA.NUM_SEQ_NF     ' +
        '        ,NFI_PRE_TRANSFERENCIA.DTA_SISTEMA    ' +
        '  from NFI_PRE_TRANSFERENCIA                  ' +
        '  WHERE DTA_MVTO >= CURRENT_DATE - 60         ' +
        '  order by                                    ' +
        '  NFI_PRE_TRANSFERENCIA.COD_EMP,              ' +
        '  NFI_PRE_TRANSFERENCIA.COD_UNIDADE,          ' +
        '  NFI_PRE_TRANSFERENCIA.DTA_MVTO,             ' +
        '  NFI_PRE_TRANSFERENCIA.COD_ITEM              ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_PRE_TRANSFERENCIA';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_PRE_TRANSFERENCIA ' +
            DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;

    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select cod_emp, cod_unidade, dta_movimento, min(num_nota) inicial, max (num_nota) final from nfi_notas    '+
                                       ' where dta_movimento >= current_date - ' + (sDiasNotas) +
                                       ' and dta_movimento < current_date          '+
                                       ' group by cod_emp, cod_unidade, dta_movimento '+
                                       ' order by dta_movimento     ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_CONTROLE_NUMERACOES.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_CONTROLE_NUMERACOES';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select cod_emp, cod_unidade, dta_movimento, min(num_nota) inicial, max (num_nota) final from nfi_notas    '+
                                       ' where dta_movimento >= current_date - ' + (sDiasNotas) +
                                       ' and dta_movimento < current_date          '+
                                       ' group by cod_emp, cod_unidade, dta_movimento '+
                                       ' order by dta_movimento     ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_CONTROLE_NUMERACOES';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select cod_emp, cod_unidade, dta_movimento, min(num_nota) inicial, max (num_nota) final from nfi_notas    '+
                                       ' where dta_movimento >= current_date - ' + (sDiasNotas) +
                                       ' and dta_movimento < current_date          '+
                                       ' group by cod_emp, cod_unidade, dta_movimento '+
                                       ' order by dta_movimento     ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_CONTROLE_NUMERACOES';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_CONTROLE_NUMERACOES ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;

    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from CRE_MVTO_REC_TEF where dta_movimento >= CURRENT_DATE - 60   ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\CRE_MVTO_REC_TEF.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'CRE_MVTO_REC_TEF';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela CRE_MVTO_REC_TEF ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from GER_CONTROLE_VERSAO where COD_UNIDADE ='+IntToStr(FrmPrincipal.iCod_unidade)+'   ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_CONTROLE_VERSAO.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'GER_CONTROLE_VERSAO';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
     }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela GER_CONTROLE_VERSAO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := 'select cod_emp, cod_unidade,dta_movimento, cod_operacao, num_cupom, num_equipamento, ind_cancelado  '+
                                           '     from est_cupons        '+
                                           '     where dta_movimento >= current_date - ' + (sDiasNotas) +
                                           '       and dta_movimento < current_date  '+
                                           '    order by dta_movimento  ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CONTROLE_CUPONS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CONTROLE_CUPONS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CONTROLE_CUPONS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;


    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := '  select cod_emp, cod_unidade, dta_movimento,cod_operacao, num_equipamento, min(num_cupom) inicial, max (num_cupom) final   '+
                                           '    from EST_CUPONS        '+
                                           '   where dta_movimento >= current_date - ' + (sDiasNotas) +
                                           '     and dta_movimento < current_date  '+
                                           '   group by  cod_emp, cod_unidade, dta_movimento,cod_operacao, num_equipamento   '+
                                           '   order by dta_movimento     ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_CONTROLE_NUMERACOES.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CONTROLE_NUMERACOES';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
       }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_CONTROLE_NUMERACOES ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select cod_emp, cod_unidade, num_seq_nf, num_nota, dta_movimento, ind_impressa  '+
                                       '  from nfi_notas               '+
                                       ' where dta_movimento >= current_date - ' + (sDiasNotas) +
                                       ' and dta_movimento < current_date     '+
                                       ' order by dta_movimento     ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_CONTROLE_NOTAS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_CONTROLE_NOTAS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;  }
       {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select cod_emp, cod_unidade, num_seq_nf, num_nota, dta_movimento, ind_impressa  '+
                                       '  from nfi_notas               '+
                                       ' where dta_movimento >= current_date - ' + (sDiasNotas) +
                                       ' and dta_movimento < current_date     '+
                                       ' order by dta_movimento     ';


      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_CONTROLE_NOTAS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL :=  ' select cod_emp, cod_unidade, num_seq_nf, num_nota, dta_movimento, ind_impressa  '+
                                        '  from nfi_notas               '+
                                        ' where dta_movimento >= current_date - ' + (sDiasNotas) + --10    '+
                                        ' and dta_movimento < current_date     '+
                                        ' order by dta_movimento     ';

      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_CONTROLE_NOTAS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_CONTROLE_NOTAS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;

    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' + ' from NFI_NOTAS ' +
                                       ' where dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' and ind_impressa = 1' ;

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_NOTAS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from NFI_NOTAS ' +
                                       ' where dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' and ind_impressa = 1' ;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from NFI_NOTAS ' +
                                       ' where dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' and ind_impressa = 1' ;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_NOTAS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
       DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' + ' from EST_NFE_CONTROLE '
        + ' where dta_movimento >= current_date -  ' +
        (sDiasNotas);
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\EST_NFE_CONTROLE.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_NFE_CONTROLE';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from EST_NFE_CONTROLE '
        + ' where dta_movimento >= current_date -  ' +
        (sDiasNotas);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_NFE_CONTROLE';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from EST_NFE_CONTROLE '
        + ' where dta_movimento >= current_date -  ' +
        (sDiasNotas);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_NFE_CONTROLE';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela EST_NFE_CONTROLE ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from NFI_NOTAS_ITENS A' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y          '+
                                       '              WHERE Y.COD_EMP = A.COD_EMP       '+
                                       '              AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '              AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '              AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '              AND Y.IND_IMPRESSA = 1)           ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_NOTAS_ITENS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_ITENS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;}
       {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from NFI_NOTAS_ITENS A' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y          '+
                                       '              WHERE Y.COD_EMP = A.COD_EMP       '+
                                       '              AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '              AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '              AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '              AND Y.IND_IMPRESSA = 1)           ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_ITENS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from NFI_NOTAS_ITENS A' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y          '+
                                       '              WHERE Y.COD_EMP = A.COD_EMP       '+
                                       '              AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '              AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '              AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '              AND Y.IND_IMPRESSA = 1)           ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_ITENS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_NOTAS_ITENS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
       DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from NFI_NOTAS_NFE A' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y     '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)           ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_NOTAS_NFE.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_NFE';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from NFI_NOTAS_NFE A' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y     '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)           ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_NFE';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from NFI_NOTAS_NFE A' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y     '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)           ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_NFE';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_NOTAS_NFE ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
       DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select *  ' +
        ' from NFI_TRANSPORTE ' + '  ';
      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_TRANSPORTE.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;

      {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_TRANSPORTE';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from NFI_TRANSPORTE ' + '  ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_TRANSPORTE';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
        ' from NFI_TRANSPORTE ' + ' ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_TRANSPORTE';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_TRANSPORTE ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
       DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from NFI_NOTAS_ICMS A ' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y     '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)           ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_NOTAS_ICMS.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_ICMS';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
       {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from NFI_NOTAS_ICMS A ' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y     '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)           ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_ICMS';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from NFI_NOTAS_ICMS A ' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y     '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)             ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_ICMS';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_NOTAS_ICMS ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    try
      DtmPGConnection.FDQuery1.Close;
      queryTodasAsLojas.Close;
      DtmPGConnection.FDQuery1.Active := False;
      DtmPGConnection.FDQuery1.sql.Text := ' select * from NFI_NOTAS_CFOP A ' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y           '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)           ';

      DtmPGConnection.FDQuery1.Active := True;
      DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
      DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\NFI_NOTAS_CFOP.json', sfJSON);
      DtmPGConnection.FDQuery1.Active := False;
      {
      queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
      DtmPGConnection.FDQuery1.Active := False;
      FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_CFOP';
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      if bHabilitaUpdate then
      begin
            FDBatchMove1.Mode := dmUpdate;
            FDBatchMove1.Execute;
      end;
      }
      {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from NFI_NOTAS_CFOP A ' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y           '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)           ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_CFOP';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select * from NFI_NOTAS_CFOP A ' +
                                       ' where A.dta_movimento >= current_date -  ' + (sDiasNotas) +
                                       ' AND EXISTS (SELECT 1 FROM NFI_NOTAS Y     '+
                                       '             WHERE Y.COD_EMP = A.COD_EMP         '+
                                       '               AND Y.COD_UNIDADE = A.COD_UNIDADE '+
                                       '               AND Y.NUM_SEQ_NF = A.NUM_SEQ_NF   '+
                                       '               AND Y.NUM_NOTA = A.NUM_NOTA       '+
                                       '               AND y.IND_IMPRESSA = 1)           ';
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'NFI_NOTAS_CFOP';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela NFI_NOTAS_CFOP ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;
    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    if ((StrToTime(TimeToStr(now)) >= StrToTime('01:00:00')) and
      (StrToTime(TimeToStr(now)) <= StrToTime('06:00:00'))) then
      begin
        try
          DtmPGConnection.FDQuery1.Close;
          queryTodasAsLojas.Close;
          DtmPGConnection.FDQuery1.Active := False;
          DtmPGConnection.FDQuery1.sql.Text := ' select *  ' +
            ' from GER_TEMPO_LOGIN ' + '  ';
          DtmPGConnection.FDQuery1.Active := True;
          DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
          DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_TEMPO_LOGIN.json', sfJSON);
          DtmPGConnection.FDQuery1.Active := False;
          {
          queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
          DtmPGConnection.FDQuery1.Active := False;
          FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
          FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
          FDBatchMoveSQLWriter1.TableName := 'GER_TEMPO_LOGIN';
          DtmFBConnection.FDConnection1.Connected := False;
          FDBatchMove1.Mode := dmAppend;
          FDBatchMove1.Execute;
          if bHabilitaUpdate then
          begin
                FDBatchMove1.Mode := dmUpdate;
                FDBatchMove1.Execute;
          end;
          }
          {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
          FDBatchMoveSQLReader1.ReadSQL := ' select *  ' +
            ' from GER_TEMPO_LOGIN ' + '  ';
          FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
          FDBatchMoveSQLWriter1.TableName := 'GER_TEMPO_LOGIN';
          FDBatchMove1.Mode := dmAppend;
          FDBatchMove1.Execute;
          FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
          FDBatchMoveSQLReader1.ReadSQL :=  ' SELECT COD_EMP           '+
                                            '       ,COD_UNIDADE       '+
                                            '       ,NUM_EQUIPAMENTO   '+
                                            '       ,COD_USUARIO       '+
                                            '       ,HORA_INICIAL      '+
                                            '       ,HORA_FINAL        '+
                                            '       ,TEMPO_LOGIN       '+
                                            '       ,TEMPO_FORMATADO   '+
                                            '       ,DTA_MOVIMENTO     '+
                                            ' FROM GER_TEMPO_LOGIN     ';
          FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
          FDBatchMoveSQLWriter1.TableName := 'GER_TEMPO_LOGIN';
          FDBatchMove1.Mode := dmUpdate;
          FDBatchMove1.Execute;
          }
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add('Replicou Tabela GER_TEMPO_LOGIN ' +
                DateTimeToStr(now));
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        except
          on E: Exception do
            begin
              try
                try
                  MyText := TStringlist.create;
                  if Fileexists('C:\sislog\INTEGRACOES\' +
                    IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                    MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                      IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                  MyText.Add(E.Message);
                  MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                    IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
                except
                  // segue a execu��o
                end;
              finally
                MyText.Free
              end;
            end;
        end;
        try
          // CONEX�O ORACLE
          DtmFBConnection.FDConnection1.Connected := False;
          DtmFBConnection.FDConnection1.DriverName := 'Ora';
          DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
          DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
          DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
          DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
          DtmFBConnection.FDConnection1.Connected := True;
          DtmMoverDados.FDQueryLojasLog.active := False;
          DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                                  + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
          DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
          DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
          DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
          DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
          DtmMoverDados.FDQueryLojasLog.execsql;
          DtmMoverDados.FDQueryLojasLog.Connection.commit;
          DtmFBConnection.FDConnection1.Connected := False;
        except
          on E: Exception do
            begin
              // Memo1.Lines.Add(E.Message);
            end;
        end;
      end;
    try
          DtmPGConnection.FDQuery1.Close;
          queryTodasAsLojas.Close;
          DtmPGConnection.FDQuery1.Active := False;
          DtmPGConnection.FDQuery1.sql.Text :=' select *  ' + ' from SEG_MVTO_ADESAO '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
          DtmPGConnection.FDQuery1.Active := True;
          DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
          DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\SEG_MVTO_ADESAO.json', sfJSON);
          DtmPGConnection.FDQuery1.Active := False;
          {
          queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
          DtmPGConnection.FDQuery1.Active := False;
          FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
          FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
          FDBatchMoveSQLWriter1.TableName := 'SEG_MVTO_ADESAO';
          DtmFBConnection.FDConnection1.Connected := False;
          FDBatchMove1.Mode := dmAppend;
          FDBatchMove1.Execute;
          if bHabilitaUpdate then
          begin
                FDBatchMove1.Mode := dmUpdate;
                FDBatchMove1.Execute;
          end;
              }
      {FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from SEG_MVTO_ADESAO '
        + ' where DTA_MOVIMENTO >= current_date - ' +
        (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'SEG_MVTO_ADESAO';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from SEG_MVTO_ADESAO ' + ' where DTA_MOVIMENTO >= current_date - ' + (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'SEG_MVTO_ADESAO';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
      DtmFBConnection.FDConnection1.Connected := False;
      }
      try
        try
          MyText := TStringlist.create;
          if Fileexists('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
          MyText.Add('Replicou Tabela SEG_MVTO_ADESAO ' + DateTimeToStr(now));
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        except
          // segue a execu��o
        end;
      finally
        MyText.Free
      end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;

    try
          DtmPGConnection.FDQuery1.Close;
          queryTodasAsLojas.Close;
          DtmPGConnection.FDQuery1.Active := False;
          DtmPGConnection.FDQuery1.sql.Text :=' select *  ' + ' from ger_configuracao_caixa ';
          DtmPGConnection.FDQuery1.Active := True;
          DtmPGConnection.FDQuery1.FetchAll;  // ABRE TODAS AS LINHAS DA CONSULTA
          DtmPGConnection.FDQuery1.SaveToFile('C:\sislog\INTEGRACOES\'+IntToStr(FrmPrincipal.iCod_Unidade)+'\GER_CONFIGURACAO_CAIXA.json', sfJSON);
          DtmPGConnection.FDQuery1.Active := False;
          {queryTodasAsLojas.Data := DtmPGConnection.FDQuery1.data;
          DtmPGConnection.FDQuery1.Active := False;
          FDBatchMoveDataSetReader1.DataSet :=  queryTodasAsLojas;
          FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
          FDBatchMoveSQLWriter1.TableName := 'GER_CONFIGURACAO_CAIXA';
          DtmFBConnection.FDConnection1.Connected := False;
          FDBatchMove1.Mode := dmAppend;
          FDBatchMove1.Execute;
          if bHabilitaUpdate then
          begin
                FDBatchMove1.Mode := dmUpdate;
                FDBatchMove1.Execute;
          end;
          }
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add('Replicou Tabela GER_CONFIGURACAO_CAIXA ' + DateTimeToStr(now));
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
    except
      on E: Exception do
        begin
          try
            try
              MyText := TStringlist.create;
              if Fileexists('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
                MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
                  IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
              MyText.Add(E.Message);
              MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
                IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
            except
              // segue a execu��o
            end;
          finally
            MyText.Free
          end;
        end;
    end;


    try
      // CONEX�O ORACLE
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Params.Add('CharacterSet=UTF8');
      DtmFBConnection.FDConnection1.Connected := True;
      DtmMoverDados.FDQueryLojasLog.active := False;
      DtmMoverDados.FDQueryLojasLog.SQL.text := ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                                              + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP').AsFloat :=  (FrmPrincipal.iCod_Emp);
      DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE').AsFloat := (FrmPrincipal.iCod_unidade);
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_INTEGRACAO').AsString := 'Replicacao Oracle';
      DtmMoverDados.FDQueryLojasLog.ParamByName('DES_EXECUCAO').AsString := 'REPLICANDO ORACLE ' + DateTimeToStr(now);
      DtmMoverDados.FDQueryLojasLog.execsql;
      DtmMoverDados.FDQueryLojasLog.Connection.commit;
      DtmFBConnection.FDConnection1.Connected := False;
    except
      on E: Exception do
        begin
          // Memo1.Lines.Add(E.Message);
        end;
    end;
    sleep(2000);
    {
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from EST_CUPONS_NFCE_AUD  where xml like ''%2021-09%'' ' +
      ' ' ;
      // (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_NFCE_AUD2';
      FDBatchMove1.Mode := dmAppend;
      FDBatchMove1.Execute;
      FDBatchMoveSQLReader1.Connection := DtmPGConnection.FDConnection1;
      // DtmFBConnection.FDConnection1;
      FDBatchMoveSQLReader1.ReadSQL := ' select *  ' + ' from EST_CUPONS_NFCE_AUD  where xml like ''%2021-09%'' ' +
      // ' ' ;
      '  ' ;
      // (FrmPrincipal.edtDiasReplicacao.text);
      FDBatchMoveSQLWriter1.Connection := DtmFBConnection.FDConnection1;
      // DtmPGConnection.FDConnection1;
      FDBatchMoveSQLWriter1.TableName := 'EST_CUPONS_NFCE_AUD2';
      FDBatchMove1.Mode := dmUpdate;
      FDBatchMove1.Execute;
    }
    try
      try
        MyText := TStringlist.create;
        if Fileexists('C:\sislog\INTEGRACOES\' +
          IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log') then
          MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
            IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
        MyText.Add('Replicou COM SUCESSO PARA O ORACLE! UHUUU ' +
          DateTimeToStr(now));
        MyText.SaveToFile('C:\sislog\INTEGRACOES\' +
          IntToStr(FrmPrincipal.iCod_unidade) + '\execucao.log');
      except
        // segue a execu��o
      end;
    finally
      MyText.Free
    end;
    DtmFBConnection.FDConnection1.Offline;
    DtmPGConnection.FDConnection1.Connected := False;
    DtmFBConnection.FDConnection1.Connected := False;
  end;
end.
