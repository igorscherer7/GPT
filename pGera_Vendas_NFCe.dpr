program pGera_Vendas_NFCe;

uses
  Vcl.Forms,
  ShellApi,
  Windows,
  UBasePrincipal in '..\..\comuns\UBasePrincipal.pas' {FrmBasePrincipal},
  UConfig in '..\..\comuns\UConfig.pas',
  UFBConnection in '..\..\comuns\UFBConnection.pas' {DtmFBConnection: TDataModule},
  UPrincipal in 'UPrincipal.pas' {FrmPrincipal},
  UExportaArquivo in 'UExportaArquivo.pas' {DtmExportaArquivo: TDataModule},
  UImportaArquivo in 'UImportaArquivo.pas' {DtmImportaArquivo: TDataModule},
  UMoverDados in 'UMoverDados.pas' {DtmMoverDados: TDataModule},
  UPGConnection in '..\..\comuns\UPGConnection.pas' {DtmPGConnection: TDataModule},
  UPing2 in 'UPing2.pas';

{$R *.res}
var
   H : Integer;
begin
   Application.Initialize;
   Application.MainFormOnTaskbar := True;
   Application.CreateForm(TFrmPrincipal, FrmPrincipal);
   Sleep(500);
   FrmPrincipal.ConfiguraAcessoAoBancoDeDados;
   H := CreateMutex(nil, False, PWideChar('pVendas_NFCe_'+(FrmPrincipal.siCod_Unidade)+'.exe') );
   if WaitForSingleObject(H,0) <> Wait_TimeOut then
   begin
        Application.ShowMainForm := False;
        Application.CreateForm(TDtmFBConnection, DtmFBConnection);
        Application.CreateForm(TDtmPGConnection, DtmPGConnection);
        Application.CreateForm(TDtmExportaArquivo, DtmExportaArquivo);
        Application.CreateForm(TDtmImportaArquivo, DtmImportaArquivo);
        Application.CreateForm(TDtmMoverDados, DtmMoverDados);
        Application.Run;
   end else
   begin
      Application.Terminate;// finaliza a aplicação ...
   end;
end.
