object DtmFBConnection: TDtmFBConnection
  OnDestroy = DataModuleDestroy
  Height = 518
  Width = 636
  object FDConnection1: TFDConnection
    Params.Strings = (
      'Database=10.110.39.2:C:\sislog\dados_servidor\dados_servidor.fdb'
      'User_Name=GRAZZ'
      'Password=sisloggrazz'
      'DriverID=FB')
    ResourceOptions.AssignedValues = [rvAutoReconnect]
    UpdateOptions.AssignedValues = [uvAutoCommitUpdates]
    ConnectedStoredUsage = []
    LoginPrompt = False
    AfterConnect = FDConnection1AfterConnect
    BeforeConnect = FDConnection1BeforeConnect
    Left = 77
    Top = 60
  end
  object FDGUIxWaitCursor1: TFDGUIxWaitCursor
    Provider = 'Forms'
    Left = 207
    Top = 60
  end
  object FDGUIxErrorDialog1: TFDGUIxErrorDialog
    Provider = 'Forms'
    Left = 207
    Top = 110
  end
  object FDPhysFBDriverLink1: TFDPhysFBDriverLink
    Left = 207
    Top = 161
  end
  object FDGUIxLoginDialog1: TFDGUIxLoginDialog
    Provider = 'Forms'
    Left = 207
    Top = 210
  end
  object FDMoniRemoteClientLink1: TFDMoniRemoteClientLink
    Tracing = True
    Left = 75
    Top = 145
  end
  object FDConnection2: TFDConnection
    Params.Strings = (
      'User_Name=SISLOGWEB'
      'Password=S1sl0gw3bAdm'
      'Database=192.168.200.70:1521/GRZPROD'
      'DriverID=Ora')
    UpdateOptions.AssignedValues = [uvAutoCommitUpdates]
    ConnectedStoredUsage = []
    LoginPrompt = False
    AfterConnect = FDConnection1AfterConnect
    BeforeConnect = FDConnection1BeforeConnect
    Left = 349
    Top = 52
  end
  object FDQuery1: TFDQuery
    Connection = FDConnection1
    Left = 376
    Top = 144
  end
  object FDConnection3: TFDConnection
    Params.Strings = (
      'Database=C:\sislog\dados_servidor\precos_atualizacoes.fdb'
      'User_Name=grazz'
      'Password=sisloggrazz'
      'DriverID=FB')
    UpdateOptions.AssignedValues = [uvAutoCommitUpdates]
    ConnectedStoredUsage = []
    LoginPrompt = False
    AfterConnect = FDConnection1AfterConnect
    BeforeConnect = FDConnection1BeforeConnect
    Left = 469
    Top = 52
  end
  object FDQuery2: TFDQuery
    Connection = FDConnection3
    Left = 467
    Top = 142
  end
  object fdQueryOracle: TFDQuery
    Connection = FDConnection1
    Left = 352
    Top = 376
  end
  object FDPhysOracleDriverLink1: TFDPhysOracleDriverLink
    VendorLib = 'C:\Oracle_DLL\oci.dll'
    Left = 96
    Top = 328
  end
  object FDQuery3: TFDQuery
    Connection = FDConnection1
    Left = 374
    Top = 216
  end
end
