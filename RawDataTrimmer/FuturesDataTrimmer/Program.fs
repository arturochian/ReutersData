open System     
open System.IO

let futuresDic = File.ReadAllLines("d:/data/cnfutures/list.csv")
                    |> Seq.skip 1
                    |> Seq.map (fun line ->
                        let items = line.Split(',')
                        (items.[0],items.[5]))
                    |> dict

// Common times
let time_morning1_start = DateTime(2000,1,1,1,0,0) //DateTime.ParseExact("01:00:00.000","HH:mm:ss.fff",null)
let time_morning1_end = DateTime(2000,1,1,2,15,0) //DateTime.ParseExact("02:15:00.000","HH:mm:ss.fff",null)
let time_morning2_start = DateTime(2000,1,1,2,30,0) //DateTime.ParseExact("02:30:00.000","HH:mm:ss.fff",null)
let time_morning2_end = DateTime(2000,1,1,3,30,0) //DateTime.ParseExact("03:30:00.000","HH:mm:ss.fff",null)
let time_afternoon_start = DateTime(2000,1,1,5,30,0) //DateTime.ParseExact("05:30:00.000","HH:mm:ss.fff",null)
let time_afternoon_end = DateTime(2000,1,1,7,0,0) //DateTime.ParseExact("07:00:00.000","HH:mm:ss.fff",null)

// Special times
let time_cfx_morning_start = DateTime(2000,1,1,1,15,0) //DateTime.ParseExact("01:15:00.000","HH:mm:ss.fff",null)
let time_cfx_morning_end = DateTime(2000,1,1,3,30,0) //DateTime.ParseExact("03:30:00.000","HH:mm:ss.fff",null)
let time_cfx_afternoon_start = DateTime(2000,1,1,5,0,0) //DateTime.ParseExact("05:00:00.000","HH:mm:ss.fff",null)
let time_cfx_afternoon_end = DateTime(2000,1,1,7,15,0) //DateTime.ParseExact("07:15:00.000","HH:mm:ss.fff",null)

let time_shf_night_start = DateTime(2000,1,1,13,0,0) //DateTime.ParseExact("13:00:00.000","HH:mm:ss.fff",null)
let time_shf_night_end1 = DateTime(2000,1,1,18,30,0) //DateTime.ParseExact("18:30:00.000","HH:mm:ss.fff",null)
let time_shf_night_end2 = DateTime(2000,1,1,17,00,0) //DateTime.ParseExact("17:00:00.000","HH:mm:ss.fff",null)

let isCommonTradingTime time =
    (time >= time_morning1_start && time <= time_morning1_end) ||
    (time >= time_morning2_start && time <= time_morning2_end) ||
    (time >= time_afternoon_start && time <= time_afternoon_end)

let isTradingTime code time = 
    match futuresDic.[code] with
        | "CFX" ->
            (time >= time_cfx_morning_start && time <= time_cfx_morning_end) ||
            (time >= time_cfx_afternoon_start && time <= time_cfx_afternoon_end)
        | "DLC" ->
            match code.[0..2] with
                | "DCP" | "DCJ" ->
                    (time >= time_shf_night_start && time <= time_shf_night_end1) ||
                    isCommonTradingTime time
                | _ ->
                    isCommonTradingTime time
        | "ZHC" ->
            isCommonTradingTime time
        | "SHF" ->
            match code.[0..2] with
                | "SHA" | "SAG" ->
                    (time >= time_shf_night_start && time <= time_shf_night_end1) ||
                    isCommonTradingTime time
                | "SAF" | "SCF" | "SZN" | "SPB" ->
                    (time >= time_shf_night_start && time <= time_shf_night_end2) ||
                    isCommonTradingTime time
                | _ ->
                    isCommonTradingTime time
        | _ -> false

let writeToFile (ms:MemoryStream) filename =
    let dir = Path.GetDirectoryName(filename)
    if not(Directory.Exists(dir)) then Directory.CreateDirectory(dir) |> ignore
    use fs = new FileStream(filename,FileMode.Append,FileAccess.Write)
    ms.WriteTo(fs)
    fs.Flush()
    fs.Close()

[<EntryPoint>]
let main folderPaths = 
    printfn "Preprocessing folder: %s" folderPaths.[0]
    printfn "Output to folder: %s" folderPaths.[1]
    printfn "Filename pattern: %s" folderPaths.[2]
    
    let files = Directory.GetFiles(folderPaths.[0],folderPaths.[2], SearchOption.AllDirectories)
    printfn "%i files found." files.Length

    printfn "Press enter to continue."
    Console.ReadLine() |> ignore

    let startTime = DateTime.Now

    files |> Array.sort
          |> Array.iteri(fun i file -> 
                printfn "%s" file

                use fs = new FileStream(file,FileMode.Open,FileAccess.Read)
                use gz = new Compression.GZipStream(fs,Compression.CompressionMode.Decompress)
                use ms_source = new MemoryStream()
                gz.CopyTo(ms_source);
                gz.Close();
                ms_source.Position <- 0L
                use reader = new StreamReader(ms_source)

                reader.ReadLine() |> ignore

                let mutable eof = reader.EndOfStream
                let mutable dir : string = null
                let mutable outputFile : string = null
                let mutable currentCode : string = null
                let mutable currentDate : string = null
                use mutable ms : MemoryStream = null
                use mutable msWriter : StreamWriter = null
                let mutable isBlockIncomplete = true

                let mutable numLine = 0
                let mutable numTradingLine = 0
                let mutable numOutputFile = 0

                while not(eof) do
                    numLine <- numLine + 1
                    let items = reader.ReadLine().Split(',')
                    eof <- reader.EndOfStream
                    let code, date, time = items.[0], items.[1], (items.[2].Split(':','.') |> Array.map(Int32.Parse))
                    let dateObj, timeObj = DateTime.ParseExact(date,"yyyyMMdd",null), DateTime(2000,1,1,time.[0],time.[1],time.[2])
                    
                    if not(currentCode = code && currentDate = date) then
                        //msWriter.WriteLine(String.Join(",", Seq.concat([| items.[0..2]; items.[5..22] |])))
                        if not(ms = null || msWriter = null) then
                            if isBlockIncomplete then
                                msWriter.Close()
                            else
                                msWriter.Flush()
                                writeToFile ms outputFile
                                numOutputFile <- numOutputFile + 1

                        currentCode <- code
                        currentDate <- date
                        dir <- sprintf "%s/%s/%s/%s" folderPaths.[1] (date.Substring(0,4)) (date.Substring(4,2)) (date.Substring(6,2))
                        outputFile <- sprintf "%s/%s-%s.csv" dir date code
                        ms <- new MemoryStream()
                        msWriter <- new StreamWriter(ms)
                        isBlockIncomplete <- true
                    
                    let isTrading = isTradingTime code timeObj

                    if isTrading then
                        let isLineIncomplete = items.[5..9] |> Array.forall String.IsNullOrWhiteSpace
                        isBlockIncomplete <- isBlockIncomplete && isLineIncomplete
                        numTradingLine <- numTradingLine + 1
                        msWriter.WriteLine(String.Join(",", Seq.concat([| items.[0..2]; items.[5..22] |])))

                if not(ms = null || msWriter = null) then
                    if isBlockIncomplete then
                        msWriter.Close()
                    else
                        msWriter.Flush()
                        writeToFile ms outputFile
                        numOutputFile <- numOutputFile + 1

                printfn "#%i/%i Finished - %i/%i trading lines processed into %i files." (i+1) files.Length numTradingLine numLine numOutputFile
        )
    printfn "Done."
    let endTime = DateTime.Now
    printfn "Start time: %A" startTime
    printfn "End time: %A" endTime
    printfn "Duration: %A" (endTime - startTime)

    Console.ReadLine() |> ignore
    0 // return an integer exit code

