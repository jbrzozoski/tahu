import enum

class QualityCode(enum.IntEnum):
	Bad = -2147483136
	Bad_AccessDenied = -2147483134
	Bad_AggregateNotFound = -2147483127
	Bad_DatabaseNotConnected = -2147483123
	Bad_Disabled = -2147483133
	Bad_Failure = -2147483121
	Bad_GatewayCommOff = -2147483125
	Bad_LicenseExceeded = -2147483130
	Bad_NotConnected = -2147483126
	Bad_NotFound = -2147483129
	Bad_OutOfRange = -2147483124
	Bad_ReadOnly = -2147483122
	Bad_ReferenceNotFound = -2147483128
	Bad_Stale = -2147483132
	Bad_TrialExpired = -2147483131
	Bad_Unauthorized = -2147483135
	Bad_Unsupported = -2147483120
	Error = -1073741056
	Error_Configuration = -1073741055
	Error_CycleDetected = -1073741044
	Error_DatabaseQuery = -1073741051
	Error_Exception = -1073741048
	Error_ExpressionEval = -1073741054
	Error_Formatting = -1073741046
	Error_IO = -1073741050
	Error_InvalidPathSyntax = -1073741047
	Error_ScriptEval = -1073741045
	Error_TagExecution = -1073741053
	Error_TimeoutExpired = -1073741049
	Error_TypeConversion = -1073741052
	Good = 192
	Good_Initial = 201
	Good_Provisional = 200
	Good_Unspecified = 0
	Good_WritePending = 2
	Uncertain = 1073742080
	Uncertain_DataSubNormal = 1073742083
	Uncertain_EngineeringUnitsExceeded = 1073742084
	Uncertain_IncompleteOperation = 1073742085
	Uncertain_InitialValue = 1073742082
	Uncertain_LastKnownValue = 1073742081

