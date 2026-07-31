defmodule Ariadne.Flow.Examples.Course.SubscribeStudentToCourse do
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Examples.Course.Events
  alias Ariadne.Flow.Examples.Course.Projections
  alias Ariadne.Flow.Projection

  defp number_of_course_subscriptions(course_id) do
    Projection.new(
      %{
        filter: %{
          types: [Events.StudentSubscribedToCourse],
          tags: [Events.course_tag(course_id)]
        },
        initial_state: 0
      },
      fn state, %Events.StudentSubscribedToCourse{}, _metadata -> state + 1 end
    )
  end

  defp number_of_student_subscriptions(student_id) do
    Projection.new(
      %{
        filter: %{
          types: [Events.StudentSubscribedToCourse],
          tags: [Events.student_tag(student_id)]
        },
        initial_state: 0
      },
      fn state, %Events.StudentSubscribedToCourse{}, _metadata -> state + 1 end
    )
  end

  defp student_subscribed_to_course?(student_id, course_id) do
    Projection.new(
      %{
        filter: %{
          types: [Events.StudentSubscribedToCourse],
          tags: [Events.student_tag(student_id), Events.course_tag(course_id)],
          only_last_event: true
        },
        initial_state: false
      },
      fn _state, %Events.StudentSubscribedToCourse{}, _metadata -> true end
    )
  end

  def command(%{student_id: student_id, course_id: course_id}) do
    Composite.new(
      %{
        course_exists?: Projections.course_exists(course_id),
        course_capacity: Projections.course_capacity(course_id),
        number_of_course_subscriptions: number_of_course_subscriptions(course_id),
        number_of_student_subscriptions: number_of_student_subscriptions(student_id),
        student_subscribed_to_course?: student_subscribed_to_course?(student_id, course_id)
      },
      fn
        %{course_exists?: false} ->
          {:error, :course_not_found}

        %{student_subscribed_to_course?: true} ->
          {:error, :student_already_subscribed_to_course}

        %{number_of_course_subscriptions: current, course_capacity: capacity}
        when current >= capacity ->
          {:error, :course_full}

        %{number_of_student_subscriptions: current} when current >= 5 ->
          {:error, :student_subscription_limit_reached}

        _ ->
          {:ok, [%Events.StudentSubscribedToCourse{student_id: student_id, course_id: course_id}]}
      end
    )
  end
end
