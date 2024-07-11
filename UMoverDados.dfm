object DtmMoverDados: TDtmMoverDados
  Height = 302
  Width = 585
  object FDBatchMove1: TFDBatchMove
    Reader = FDBatchMoveDataSetReader1
    Writer = FDBatchMoveSQLWriter1
    Mode = dmAppendUpdate
    Mappings = <>
    LogFileName = 'Data.log'
    CommitCount = 5000
    Left = 234
    Top = 159
  end
  object FDBatchMoveSQLReader1: TFDBatchMoveSQLReader
    Connection = DtmFBConnection.FDConnection1
    Left = 70
    Top = 88
  end
  object FDBatchMoveSQLWriter1: TFDBatchMoveSQLWriter
    Connection = DtmPGConnection.FDConnection1
    Left = 69
    Top = 227
  end
  object FDConnection1: TFDConnection
    Params.Strings = (
      'Database=192.168.200.110:1522/GRZPROD'
      'User_Name=SISLOGWEB'
      'Password=S1sl0gw3bAdm'
      'DriverID=Ora')
    LoginPrompt = False
    Left = 233
    Top = 89
  end
  object FDQueryLojasLog: TFDQuery
    Connection = DtmFBConnection.FDConnection1
    Left = 235
    Top = 228
  end
  object FDBatchMoveDataSetReader1: TFDBatchMoveDataSetReader
    Left = 70
    Top = 159
  end
  object queryTodasAsLojas: TFDMemTable
    FetchOptions.AssignedValues = [evMode]
    FetchOptions.Mode = fmAll
    ResourceOptions.AssignedValues = [rvSilentMode]
    ResourceOptions.SilentMode = True
    UpdateOptions.AssignedValues = [uvCheckRequired, uvAutoCommitUpdates]
    UpdateOptions.CheckRequired = False
    UpdateOptions.AutoCommitUpdates = True
    Left = 69
    Top = 18
  end
  object FDBatchMoveJSONWriter1: TFDBatchMoveJSONWriter
    DataDef.Fields = <>
    Left = 233
    Top = 18
  end
  object querylojas2: TFDMemTable
    FetchOptions.AssignedValues = [evMode]
    FetchOptions.Mode = fmAll
    ResourceOptions.AssignedValues = [rvSilentMode]
    ResourceOptions.SilentMode = True
    UpdateOptions.AssignedValues = [uvCheckRequired, uvAutoCommitUpdates]
    UpdateOptions.CheckRequired = False
    UpdateOptions.AutoCommitUpdates = True
    Left = 379
    Top = 15
  end
  object FDQuery1: TFDQuery
    Connection = DtmFBConnection.FDConnection1
    Left = 379
    Top = 89
  end
end
