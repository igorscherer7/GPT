unit UPrincipal;
interface
uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants,
  System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, UBasePrincipal, Vcl.StdCtrls,
  Vcl.Buttons, Vcl.ComCtrls, FireDAC.Stan.Intf, FireDAC.Stan.Option,
  System.IOUtils,
  Winapi.ShellAPI,
  Xml.XMLDoc,
  Xml.XMLIntf,
  FireDAC.Stan.Param, FireDAC.Stan.Error, FireDAC.DatS, FireDAC.Phys.Intf,
  FireDAC.DApt.Intf, FireDAC.Stan.Async, FireDAC.DApt, Data.DB, System.Zip,
  FireDAC.Comp.BatchMove.Text, FireDAC.Comp.BatchMove, IniFiles,
  FireDAC.Comp.BatchMove.DataSet, Vcl.ExtCtrls, Vcl.Grids, Vcl.DBGrids,
  FireDAC.Comp.DataSet, FireDAC.Comp.Client, REST.Types, REST.Client,
  Data.Bind.Components, Data.Bind.ObjectScope, IdBaseComponent, IdComponent,
  IdIPWatch;
type
  TFrmPrincipal = class(TFrmBasePrincipal)
    PageControl2: TPageControl;
    TabSheet3: TTabSheet;
    TabSheet4: TTabSheet;
    TabSheet5: TTabSheet;
    Panel1: TPanel;
    DBGrid1: TDBGrid;
    MemSQLSelecionar: TMemo;
    Panel2: TPanel;
    BtnExpArqExecutaSQL: TSpeedButton;
    DtsExporta: TDataSource;
    BtnExpArqSalvarTXT: TSpeedButton;
    Panel3: TPanel;
    DBGrid2: TDBGrid;
    BtnImpArquivo: TSpeedButton;
    DtsBatchMove: TDataSource;
    BtnImpArquivoAbrir: TSpeedButton;
    BtnMoverDados: TSpeedButton;
    BtnExpArqSalvarJSON: TSpeedButton;
    Memo1: TMemo;
    Timer1: TTimer;
    edtDiasReplicacao: TEdit;
    RESTClient1: TRESTClient;
    RESTRequest1: TRESTRequest;
    RESTResponse1: TRESTResponse;
    IdIPWatch1: TIdIPWatch;
    procedure FormCreate(Sender: TObject);
    procedure BtnExpArqExecutaSQLClick(Sender: TObject);
    procedure BtnExpArqSalvarTXTClick(Sender: TObject);
    procedure BtnImpArquivoClick(Sender: TObject);
    procedure BtnImpArquivoAbrirClick(Sender: TObject);
    procedure BtnMoverDadosClick(Sender: TObject);
    procedure BtnExpArqSalvarJSONClick(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure Timer1Timer(Sender: TObject);
    procedure ConfiguraAcessoAoBancoDeDados;
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
  private
    { Private declarations }
  public
    { Public declarations }
  var
    ifArqINI: TIniFile; // Arquivo INI...
    iCod_Emp, iCod_Unidade, iEquipamento: Integer;
    siCod_Unidade: String;
    sDatabase_Serv: String;
    sNomeDaMaquina : String;
    oConnGlobal: TFDConnection;
    SQLAuxiliar: TFDQuery;
    SQLAuxiliar02: TFDQuery;
    SQLAuxiliar03: TFDQuery;
    SQLAuxiliar04: TFDQuery;
    SQLAuxiliarApiEndPoints: TFDQuery;
    SQLAuxiliarVerificaBonusNovos: TFDQuery;
    SQLAuxiliarLog: TFDQuery;
  const
    NOME_CONEXAO_FIREBIRD = 'CONEXAO_SERVIDOR_CONVERSAO';
    sPathArquivoINI = 'C:\SisLog\';
    ARQUIVO_LOG_MAXSIZE = 524288;
    // Mudado para 1MB  para tentar pegar o erro do cupom cancelado
    ARQUIVO_LOG_TAM_REC = 524288;
    // Mudado para 5MB  para tentar pegar o erro do cupom cancelado
    ARQUIVO_LOG_TEF_MAXSIZE = 524288; // 1MB
  end;
var
  FrmPrincipal: TFrmPrincipal;
implementation
uses
  UFBConnection, UEXportaArquivo, UImportaArquivo, UMoverDados, UPGConnection;
{$R *.dfm}
procedure TFrmPrincipal.ConfiguraAcessoAoBancoDeDados;
var
  i: Integer;
  oParametros: TStringList;
  stringList: TStringList;
    buffer: array[0..255] of char;
    size: dword;
begin
  ifArqINI := TIniFile.Create(ExtractFileDir(Application.ExeName) +
    '\Config.ini');
  iCod_Emp := ifArqINI.ReadInteger('Empresa', 'Empresa', 2);
  iCod_Unidade := ifArqINI.ReadInteger('Unidade', 'Unidade', 1);
  iEquipamento := ifArqINI.ReadInteger('Equipamento', 'Equipamento', 1);
  siCod_Unidade := IntToStr(iCod_Unidade);
  try
      size:=256;
      if GetComputerName(buffer, size) then
      begin
           sNomeDaMaquina := Trim( buffer);
      end;
  except
       //segue a execução
  end;

  sDatabase_Serv := ifArqINI.ReadString('Server Dados_Servidor',
    'Database Dados_Servidor',
    'servidor:C:\sislog\dados_servidor\dados_servidor.fdb');
 // sIPReplicacao :=  idipwatch1.PreviousIP;
  ifArqINI.Free;
end;

procedure ExecuteMyExecutable(const ExePath: string);
begin
  if FileExists(ExePath) then
  begin
    ShellExecute(0, 'open', PChar(ExePath), nil, nil, SW_SHOWNORMAL);
  end
  else
  begin
   // ShowMessage('O arquivo ' + ExePath + ' não foi encontrado.');
  end;
end;
function GetExecutableName(const ProjectFile: string): string;
var
  XMLDoc: IXMLDocument;
  Node: IXMLNode;
begin
  Result := '';
  XMLDoc := LoadXMLDocument(ProjectFile);
  Node := XMLDoc.DocumentElement.ChildNodes.FindNode('PropertyGroup').ChildNodes.FindNode('DCC_ExeOutput');
  if Assigned(Node) and Node.HasChildNodes then
    Result := Node.ChildNodes[0].Text;
end;


procedure TFrmPrincipal.FormClose(Sender: TObject; var Action: TCloseAction);
begin

    // ExecuteMyExecutable('pVendas_NFCe_'+(FrmPrincipal.siCod_Unidade)+'.exe');
end;

procedure TFrmPrincipal.FormCreate(Sender: TObject);
var
  fs: TFileStream;
  rs: TResourceStream;
  s: String;
  z: TZipFile;
  MyText: TStringList;
begin
  try
    inherited;
    ConfiguraAcessoAoBancoDeDados;
    // BtnMoverDadosClick(Sender);
    // PageControl2.ActivePageIndex := 0;
    { try
      DeleteFile('C:\sislog\dados_servidor\precos_atualizacao.fdb');
      except
      end;
      rs := TResourceStream.Create(hInstance, 'zip_base', RT_RCDATA);
      s  := 'C:\sislog\dados_servidor\precos_atualizacao.zip';
      fs := TFileStream.Create(s,fmCreate);
      rs.SaveToStream(fs);
      rs.Free;
      fs.Free;
      try
      z := TZipFile.Create;
      if fileExists('C:\sislog\dados_servidor\precos_atualizacao.zip') then
      z.Open('C:\sislog\dados_servidor\precos_atualizacao.zip', zmReadWrite)
      else
      raise exception.Create('Não encontrei: ' + 'C:\sislog\dados_servidor\precos_atualizacao.zip');
      z.ExtractAll('C:\sislog\dados_servidor\');
      z.Close;
      finally
      z.Free;
      end;
    }
    try
      System.IOUtils.TFile.Delete('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) + '\execucao.log');
    except
      // segue
    end;
    try
      MyText := TStringList.Create;
      MyText.Add('iniciando LOG Replicação FireBird > Oracle ');
      MyText.Add('emp =' + IntToStr(iCod_Emp) + '    ');
      MyText.Add('loja =' + IntToStr(iCod_Unidade) + '    ');
      MyText.Add('servidor =' + sDatabase_Serv + '    ');
      MyText.Add('          ');
      MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log');
    finally
      MyText.Free
    end; { try }
  except
    if Assigned(DtmMoverDados) then
    begin
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      FreeAndNil(DtmMoverDados);
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    if Assigned(DtmFBConnection) then
    begin
      try
        DtmFBConnection.FDConnection1.Close;
      except
      end;
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      Try
        FreeAndNil(DtmFBConnection);
      except
        try
          FreeAndNil(DtmFBConnection);
        except
        end;
      End;
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    if Assigned(DtmPGConnection) then
    begin
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      // FreeAndNil(DtmPGConnection);
      Try
        FreeAndNil(DtmPGConnection);
      except
      End;
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    try
      self.Free; // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    try
      Application.Free;
      // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    try
      exitprocess(0);
      // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    Application.Terminate;
    Close;
  end;
end;
procedure TFrmPrincipal.FormShow(Sender: TObject);
var
  sSQL: String;
begin
  inherited;
  { DtmFBConnection.FDConnection2.Connected := True;
    DtmFBConnection.FDConnection3.Connected := True;
    DtmFBConnection.FDQuery1.Active := False;
    DtmFBConnection.FDQuery1.sql.text :=  ' select cod_emp,  '+
    ' cod_unidade,    '+
    ' cod_item,       '+
    ' to_char(DTA_PRECO_LOJA,''mm/dd/yyyy'') as DTA_PRECO_LOJA, '+
    ' to_char(DTA_PRECO_LOJA_FIM,''mm/dd/yyyy'') as DTA_PRECO_LOJA_FIM, '+
    ' VLR_PRECO_LOJA FROM sislogweb.grz_ite_precos_loja ';
    DtmFBConnection.FDQuery1.Active := True;
    DtmFBConnection.FDQuery1.first;
    while not DtmFBConnection.FDQuery1.eof do
    begin
    sSQL:=  '';
    sSQL:=  ' update ITE_PRECOS set DTA_PRECO_LOJA = :DTA_PRECO_LOJA ';
    if DtmFBConnection.FDQuery1.fieldbyname('VLR_PRECO_LOJA'). then

    DtmFBConnection.FDQuery2.Active := False;
    DtmFBConnection.FDQuery2.sql.text := ' select * FROM sislogweb.grz_ite_precos_loja ';
    DtmFBConnection.FDQuery2.Active := True;
    DtmFBConnection.FDQuery1.next;
    end;
  }
end;
procedure TFrmPrincipal.Timer1Timer(Sender: TObject);
var
  MyText: TStringList;
begin
  inherited;
  try
    Timer1.Enabled := False;
    Timer1.Interval := 600000; // 2,5  horas
    try
      BtnMoverDadosClick(Sender);
    except
      on E: Exception do
      begin
        // segue a execução
        MyText := TStringList.Create;
        try
          MyText.Add('erro log  ');
          MyText.Add('erro =' + E.Message + '    ');
          MyText.Add('          ');
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
            '\execucao.log');
        finally
          MyText.Free
        end; { try }
      end;
    end;
  finally
    Timer1.Enabled := True;
  end;
end;
procedure TFrmPrincipal.BtnExpArqExecutaSQLClick(Sender: TObject);
begin
  DtmExportaArquivo.SelecionarDados(MemSQLSelecionar.Text);
end;
procedure TFrmPrincipal.BtnExpArqSalvarJSONClick(Sender: TObject);
begin
  DtmExportaArquivo.SalvarArquivoJSON;
end;
procedure TFrmPrincipal.BtnExpArqSalvarTXTClick(Sender: TObject);
begin
  DtmExportaArquivo.SalvarArquivoTexto;
end;
procedure TFrmPrincipal.BtnImpArquivoAbrirClick(Sender: TObject);
begin
  DtmImportaArquivo.QryBatchMove.Close;
  DtmImportaArquivo.QryBatchMove.Open;
end;
procedure TFrmPrincipal.BtnImpArquivoClick(Sender: TObject);
begin
  DtmImportaArquivo.ImportarArquivo;
end;
procedure TFrmPrincipal.BtnMoverDadosClick(Sender: TObject);
var
  MyText: TStringList;
  tfArquivo: TextFile;
  sLinha: string;
begin

while true do
begin



  if ((StrToTime(TimeToStr(now)) >= StrToTime('00:00:00')) and
    (StrToTime(TimeToStr(now)) <= StrToTime('03:00:00'))) then
  begin
    // nao achou config destroi tudo
    if Assigned(DtmMoverDados) then
    begin
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      FreeAndNil(DtmMoverDados);
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    if Assigned(DtmFBConnection) then
    begin
      try
        DtmFBConnection.FDConnection1.Close;
      except
      end;
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      Try
        FreeAndNil(DtmFBConnection);
      except
        try
          FreeAndNil(DtmFBConnection);
        except
        end;
      End;
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    if Assigned(DtmPGConnection) then
    begin
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      // FreeAndNil(DtmPGConnection);
      Try
        FreeAndNil(DtmPGConnection);
      except
      End;
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    try
      self.Free; // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    try
      Application.Free;
      // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    try
      exitprocess(0);
      // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    Application.Terminate;
    Close;
  end;
  if iCod_Unidade = 1 then
  begin
    // nao achou config destroi tudo
    if Assigned(DtmMoverDados) then
    begin
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      FreeAndNil(DtmMoverDados);
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    if Assigned(DtmFBConnection) then
    begin
      try
        DtmFBConnection.FDConnection1.Close;
      except
      end;
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      Try
        FreeAndNil(DtmFBConnection);
      except
        try
          FreeAndNil(DtmFBConnection);
        except
        end;
      End;
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    if Assigned(DtmPGConnection) then
    begin
      { if ClientModule2.SQLConnection1.Connected then
        begin
        ClientModule2.SQLConnection1.Connected:= False;
        end; }
      // objeto já criado em memória
      // FreeAndNil(DtmPGConnection);
      Try
        FreeAndNil(DtmPGConnection);
      except
      End;
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end
    else
    begin
      // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
    end;
    try
      self.Free; // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    try
      Application.Free;
      // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    try
      exitprocess(0);
      // usado somente se estiver dentro de uma form ou datamodule
    except
      //
    end;
    Application.Terminate;
    Close;
  end;
  try
    Memo1.Lines.Clear;
    Memo1.Lines.Add('inicalizado em ' + DateTimeToStr(now));
    try
      MyText := TStringList.Create;
      if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log') then
        MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
          '\execucao.log');
      MyText.Add('inicalizado em ' + DateTimeToStr(now));
      MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log');
    finally
      MyText.Free;
    end; { try }
  except
    // segue a execução
  end;
  DtmFBConnection.FDConnection1.Connected := False;
  DtmPGConnection.FDConnection1.Connected := False;
  Memo1.Lines.Add('finalizado em ' + DateTimeToStr(now));
  try
    try
      MyText := TStringList.Create;
      if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log') then
        MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
          '\execucao.log');
      MyText.Add('finalizado em ' + DateTimeToStr(now));
      MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log');
    finally
      MyText.Free;
    end; { try }
  except
    // segue a execução
  end;
  try
    Memo1.Lines.Clear;
    Memo1.Lines.Add('inicalizado Oracle em ' + DateTimeToStr(now));
    try
      MyText := TStringList.Create;
      if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log') then
        MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
          '\execucao.log');
      MyText.Add('inicalizado Oracle em ' + DateTimeToStr(now));
      MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log');
    finally
      MyText.Free;
    end; { try }
  except
    // segue a execução
  end;
  try
    DtmMoverDados.MoverDadosVendasNFCe;
  except
    on E: Exception do
    begin
      try
        try
          MyText := TStringList.Create;
          if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
            '\execucao.log') then
            MyText.loadFromFile('C:\sislog\INTEGRACOES\' +
              IntToStr(iCod_Unidade) + '\execucao.log');
          MyText.Add('erro : ' + E.Message);
          MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
            '\execucao.log');
        finally
          MyText.Free;
        end; { try }
      except
        // segue a execução
      end;
    end;
  end;
  try
    try
      MyText := TStringList.Create;
      if Fileexists('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log') then
        MyText.loadFromFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
          '\execucao.log');
      MyText.Add('finalizado Oracle em ' + DateTimeToStr(now));
      MyText.SaveToFile('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log');
    finally
      MyText.Free;
    end; { try }
  except
    // segue a execução
  end;
  try
      DtmFBConnection.FDConnection1.Connected := False;
      DtmFBConnection.FDConnection1.DriverName := 'Ora';
      DtmFBConnection.FDConnection1.Params.USERNAME := 'SISLOGWEB';
      DtmFBConnection.FDConnection1.Params.Password := 'S1sl0gw3bAdm';
      DtmFBConnection.FDConnection1.Params.database := '192.168.200.110:1522/GRZPROD';
      DtmFBConnection.FDConnection1.Connected := True;
    except
          //segue
    end;
  try
       if FileExists('C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +
        '\execucao.log') then
                Begin
                  AssignFile(tfArquivo, 'C:\sislog\INTEGRACOES\' + IntToStr(iCod_Unidade) +'\execucao.log');
                  Reset(tfArquivo);
                  While not eof(tfArquivo) do
                  Begin
                    ReadLn(tfArquivo, sLinha);
                    if Trim(sLinha) <> '' then
                    begin

                      Application.ProcessMessages;
                      try
                        DtmMoverDados.FDQueryLojasLog.active := False;
                        DtmMoverDados.FDQueryLojasLog.SQL.Text :=
                          ' insert into SISLOGWEB.GRZ_MVTO_INTEGRACOES (COD_EMP,COD_UNIDADE,DES_INTEGRACAO,DES_EXECUCAO) '
                          + ' values (:COD_EMP,:COD_UNIDADE,:DES_INTEGRACAO,:DES_EXECUCAO) ';
                        DtmMoverDados.FDQueryLojasLog.ParamByName('COD_EMP')
                          .AsFloat := iCod_Emp;
                        DtmMoverDados.FDQueryLojasLog.ParamByName('COD_UNIDADE')
                          .AsFloat := iCod_Unidade;
                        DtmMoverDados.FDQueryLojasLog.ParamByName
                          ('DES_INTEGRACAO').AsString := 'Log FB Loja';
                        DtmMoverDados.FDQueryLojasLog.ParamByName
                          ('DES_EXECUCAO').AsString := copy(sLinha, 1, 248);
                        DtmMoverDados.FDQueryLojasLog.execsql;
                        DtmMoverDados.FDQueryLojasLog.Connection.commit;
                      except
                        on E: Exception do
                        begin
                          Memo1.Lines.Add(E.Message);
                        end;
                      end;
                      Sleep(500);
                    end;
                  end;

                  CloseFile(tfArquivo);
                end;
  except
                // segue a execução

  end;
   //ExecuteMyExecutable('pVendas_NFCe_'+(FrmPrincipal.siCod_Unidade)+'.exe');

  if Assigned(DtmMoverDados) then
  begin
    { if ClientModule2.SQLConnection1.Connected then
      begin
      ClientModule2.SQLConnection1.Connected:= False;
      end; }
    // objeto já criado em memória
    FreeAndNil(DtmMoverDados);
    // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
  end
  else
  begin
    // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
  end;
  if Assigned(DtmFBConnection) then
  begin
    try
      DtmFBConnection.FDConnection1.Close;
    except
    end;
    { if ClientModule2.SQLConnection1.Connected then
      begin
      ClientModule2.SQLConnection1.Connected:= False;
      end; }
    // objeto já criado em memória
    Try
      FreeAndNil(DtmFBConnection);
    except
      try
        FreeAndNil(DtmFBConnection);
      except
      end;
    End;
    // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
  end
  else
  begin
    // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
  end;
  if Assigned(DtmPGConnection) then
  begin
    { if ClientModule2.SQLConnection1.Connected then
      begin
      ClientModule2.SQLConnection1.Connected:= False;
      end; }
    // objeto já criado em memória
    // FreeAndNil(DtmPGConnection);
    Try
      FreeAndNil(DtmPGConnection);
    except
    End;
    // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
  end
  else
  begin
    // Application.CreateForm(TDtmMoverDados, DtmMoverDados);
  end;
  try
    self.Free; // usado somente se estiver dentro de uma form ou datamodule
  except
    //
  end;
  try
    Application.Free;
    // usado somente se estiver dentro de uma form ou datamodule
  except
    //
  end;
  try
    exitprocess(0); // usado somente se estiver dentro de uma form ou datamodule
  except
    //
  end;
    Sleep(1500);
end;


 // Application.Terminate;
 // Close;



  // Sleep (1000000);
  // end;
end;
end.
