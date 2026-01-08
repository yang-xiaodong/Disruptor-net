using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Disruptor.Util;

namespace Disruptor.Processing;

/// <summary>
/// AOT-compatible IpcEventProcessor that does not use dynamic generic construction.
/// Slightly lower performance than the optimized version, but works correctly in AOT environments.
/// </summary>
internal class AotIpcEventProcessor<T> : IIpcEventProcessor<T>
    where T : unmanaged
{
    private readonly IpcRingBuffer<T> _dataProvider;
    private readonly SequencePointer _sequence;
    private readonly IpcSequenceBarrier _sequenceBarrier;
    private readonly IValueEventHandler<T> _eventHandler;
    private readonly EventProcessorState _state;
    private IValueExceptionHandler<T> _exceptionHandler = new ValueFatalExceptionHandler<T>();

    public AotIpcEventProcessor(
        IpcRingBuffer<T> dataProvider,
        SequencePointer sequence,
        IpcSequenceBarrier sequenceBarrier,
        IValueEventHandler<T> eventHandler)
    {
        _dataProvider = dataProvider;
        _sequence = sequence;
        _sequenceBarrier = sequenceBarrier;
        _eventHandler = eventHandler;
        _state = new EventProcessorState(sequenceBarrier, restartable: true);
    }

    public SequencePointer SequencePointer => _sequence;

    public Task Halt() => _state.Halt();

    public void Dispose() => _state.Dispose().Wait();

    public ValueTask DisposeAsync() => new ValueTask(_state.Dispose());

    public bool IsRunning => _state.IsRunning;

    public void SetExceptionHandler(IValueExceptionHandler<T> exceptionHandler)
    {
        _exceptionHandler = exceptionHandler ?? throw new ArgumentNullException(nameof(exceptionHandler));
    }

    public Task Start(TaskScheduler taskScheduler)
    {
        var runState = _state.Start();
        taskScheduler.StartLongRunningTask(() => Run(runState));
        return runState.StartTask;
    }

    private void Run(EventProcessorState.RunState runState)
    {
        NotifyStart(runState);
        try
        {
            ProcessEvents(runState.CancellationToken);
        }
        finally
        {
            NotifyShutdown(runState);
        }
    }

    [MethodImpl(Constants.AggressiveOptimization)]
    private void ProcessEvents(CancellationToken cancellationToken)
    {
        var nextSequence = _sequence.Value + 1L;

        while (true)
        {
            try
            {
                var waitResult = _sequenceBarrier.WaitFor(nextSequence, cancellationToken);
                if (waitResult.IsTimeout)
                {
                    NotifyTimeout();
                    continue;
                }

                var availableSequence = waitResult.UnsafeAvailableSequence;

                _eventHandler.OnBatchStart(availableSequence - nextSequence + 1);

                while (nextSequence <= availableSequence)
                {
                    ref T evt = ref _dataProvider[nextSequence];
                    _eventHandler.OnEvent(ref evt, nextSequence, nextSequence == availableSequence);
                    nextSequence++;
                }

                _sequence.SetValue(availableSequence);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                ref T evt = ref _dataProvider[nextSequence];
                _exceptionHandler.HandleEventException(ex, nextSequence, ref evt);
                _sequence.SetValue(nextSequence);
                nextSequence++;
            }
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void NotifyTimeout()
    {
        try
        {
            _eventHandler.OnTimeout(_sequence.Value);
        }
        catch (Exception ex)
        {
            _exceptionHandler.HandleOnTimeoutException(ex, _sequence.Value);
        }
    }

    private void NotifyStart(EventProcessorState.RunState runState)
    {
        try
        {
            _eventHandler.OnStart();
        }
        catch (Exception e)
        {
            _exceptionHandler.HandleOnStartException(e);
        }
        runState.OnStarted();
    }

    private void NotifyShutdown(EventProcessorState.RunState runState)
    {
        try
        {
            _eventHandler.OnShutdown();
        }
        catch (Exception e)
        {
            _exceptionHandler.HandleOnShutdownException(e);
        }
        runState.OnShutdown();
    }
}
