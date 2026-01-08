using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Disruptor.Util;

namespace Disruptor.Processing;

/// <summary>
/// AOT-compatible ValueEventProcessor that does not use dynamic generic construction.
/// Slightly lower performance than the optimized version, but works correctly in AOT environments.
/// </summary>
public class AotValueEventProcessor<T> : IValueEventProcessor<T>
    where T : struct
{
    private readonly IValueDataProvider<T> _dataProvider;
    private readonly SequenceBarrier _sequenceBarrier;
    private readonly IValueEventHandler<T> _eventHandler;
    private readonly Sequence _sequence = new();
    private readonly EventProcessorState _state;
    private IValueExceptionHandler<T> _exceptionHandler = new ValueFatalExceptionHandler<T>();

    public AotValueEventProcessor(
        IValueDataProvider<T> dataProvider,
        SequenceBarrier sequenceBarrier,
        IValueEventHandler<T> eventHandler)
    {
        _dataProvider = dataProvider;
        _sequenceBarrier = sequenceBarrier;
        _eventHandler = eventHandler;
        _state = new EventProcessorState(sequenceBarrier, restartable: true);

        if (eventHandler is IEventProcessorSequenceAware sequenceAware)
            sequenceAware.SetSequenceCallback(_sequence);
    }

    public Sequence Sequence => _sequence;

    public Task Halt() => _state.Halt();

    public void Dispose() => _state.Dispose();

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

/// <summary>
/// AOT-compatible high-performance ValueEventProcessor that avoids interface call overhead through generic parameters.
/// Requires manual instantiation with concrete types.
/// </summary>
/// <typeparam name="T">The type of event used.</typeparam>
/// <typeparam name="TDataProvider">The type of the data provider.</typeparam>
/// <typeparam name="TEventHandler">The type of the event handler.</typeparam>
public class AotValueEventProcessor<T, TDataProvider, TEventHandler> : IValueEventProcessor<T>
    where T : struct
    where TDataProvider : IValueDataProvider<T>
    where TEventHandler : IValueEventHandler<T>
{
    private TDataProvider _dataProvider;
    private readonly SequenceBarrier _sequenceBarrier;
    private TEventHandler _eventHandler;
    private readonly Sequence _sequence = new();
    private readonly EventProcessorState _state;
    private IValueExceptionHandler<T> _exceptionHandler = new ValueFatalExceptionHandler<T>();

    public AotValueEventProcessor(
        TDataProvider dataProvider,
        SequenceBarrier sequenceBarrier,
        TEventHandler eventHandler)
    {
        _dataProvider = dataProvider;
        _sequenceBarrier = sequenceBarrier;
        _eventHandler = eventHandler;
        _state = new EventProcessorState(sequenceBarrier, restartable: true);

        if (eventHandler is IEventProcessorSequenceAware sequenceAware)
            sequenceAware.SetSequenceCallback(_sequence);
    }

    public Sequence Sequence => _sequence;

    public Task Halt() => _state.Halt();

    public void Dispose() => _state.Dispose();

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
