object DtmPGConnection: TDtmPGConnection
  Height = 335
  Width = 343
  object FDConnection1: TFDConnection
    Params.Strings = (
      'Database=C:\SISLOG\DADOS_SERVIDOR\DADOS_SERVIDOR.FDB'
      'User_Name=grazz'
      'Password=sisloggrazz'
      'DriverID=FB')
    ResourceOptions.AssignedValues = [rvAutoReconnect]
    UpdateOptions.AssignedValues = [uvAutoCommitUpdates]
    ConnectedStoredUsage = []
    LoginPrompt = False
    BeforeConnect = FDConnection1BeforeConnect
    Left = 92
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
  object FDGUIxLoginDialog1: TFDGUIxLoginDialog
    Provider = 'Forms'
    Left = 207
    Top = 210
  end
  object FDPhysPgDriverLink1: TFDPhysPgDriverLink
    Left = 205
    Top = 160
  end
  object FDConnection2: TFDConnection
    Params.Strings = (
      'Database=C:\SISLOG\DADOS_SERVIDOR\DADOS_SERVIDOR.FDB'
      'User_Name=grazz'
      'Password=sisloggrazz'
      'DriverID=FB')
    UpdateOptions.AssignedValues = [uvAutoCommitUpdates]
    ConnectedStoredUsage = []
    LoginPrompt = False
    BeforeConnect = FDConnection1BeforeConnect
    Left = 92
    Top = 156
  end
  object FDQuery1: TFDQuery
    Connection = FDConnection1
    Left = 48
    Top = 256
  end
end
